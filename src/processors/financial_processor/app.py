# src/processors/financial_processor/app.py
import os
import json
import boto3
import logging
import sys
import re
import time
from datetime import datetime
import traceback
from common.confidence_utils import evaluate_confidence, mark_for_manual_review

# Agregar las rutas para importar módulos comunes
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
sys.path.append('/opt')

from common.db_connector import (
    update_document_extraction_data,
    update_document_processing_status,
    get_document_by_id,
    insert_analysis_record,
    generate_uuid,
    link_document_to_client
)
from financial_parser import parse_financial_document, extract_transactions

# Configurar el logger
logger = logging.getLogger()
logger.setLevel(os.environ.get('LOG_LEVEL', 'INFO'))

# Configuración de reintentos para clientes AWS
from botocore.config import Config
retry_config = Config(
    retries={
        'max_attempts': 3,
        'mode': 'standard'
    }
)

# Instanciar clientes de AWS (solo para AWS SDK, no Textract/Comprehend)
sqs_client = boto3.client('sqs', config=retry_config)

def get_extracted_data_from_db(document_id):
    """
    Recupera los datos ya extraídos por textract_callback de la base de datos.
    Centraliza la extracción para simplificar el manejo de errores.
    """
    try:
        start_time = time.time()
        # Obtener documento
        document_data = get_document_by_id(document_id)
        
        if not document_data:
            logger.error(f"No se encontró el documento {document_id} en la base de datos")
            return None
        
        # Obtener los datos extraídos del campo JSON
        extracted_data = {}
        if document_data.get('datos_extraidos_ia'):
            try:
                # Si ya es un diccionario, usarlo directamente
                if isinstance(document_data['datos_extraidos_ia'], dict):
                    extracted_data = document_data['datos_extraidos_ia']
                else:
                    # Si es una cadena JSON, deserializarla
                    extracted_data = json.loads(document_data['datos_extraidos_ia'])
            except json.JSONDecodeError:
                logger.error(f"Error al decodificar datos_extraidos_ia para documento {document_id}")
                return None
        
        # Obtener texto extraído y datos analizados
        query = """
        SELECT texto_extraido, entidades_detectadas, metadatos_extraccion, estado_analisis, tipo_documento
        FROM analisis_documento_ia
        WHERE id_documento = %s
        ORDER BY fecha_analisis DESC
        LIMIT 1
        """
        
        from common.db_connector import execute_query
        analysis_results = execute_query(query, (document_id,))
        
        if not analysis_results:
            logger.warning(f"No se encontró análisis en base de datos para documento {document_id}")
            # Continuar con lo que tengamos en datos_extraidos_ia
        else:
            analysis_data = analysis_results[0]
            
            # Agregar texto completo
            if analysis_data.get('texto_extraido'):
                extracted_data['texto_completo'] = analysis_data['texto_extraido']
            
            # Agregar entidades detectadas
            if analysis_data.get('entidades_detectadas'):
                try:
                    entidades = json.loads(analysis_data['entidades_detectadas']) if isinstance(analysis_data['entidades_detectadas'], str) else analysis_data['entidades_detectadas']
                    extracted_data['entidades'] = entidades
                except json.JSONDecodeError:
                    logger.warning(f"Error al decodificar entidades_detectadas para documento {document_id}")
            
            # Agregar metadatos de extracción
            if analysis_data.get('metadatos_extraccion'):
                try:
                    metadatos = json.loads(analysis_data['metadatos_extraccion']) if isinstance(analysis_data['metadatos_extraccion'], str) else analysis_data['metadatos_extraccion']
                    extracted_data['metadatos_extraccion'] = metadatos
                except json.JSONDecodeError:
                    logger.warning(f"Error al decodificar metadatos_extraccion para documento {document_id}")
            
            # Agregar tipo de documento detectado
            if analysis_data.get('tipo_documento'):
                extracted_data['tipo_documento_detectado'] = analysis_data['tipo_documento']
        
        # Registrar tiempo de consulta
        logger.info(f"Datos recuperados para documento {document_id} en {time.time() - start_time:.2f} segundos")
        
        return {
            'document_id': document_id,
            'document_data': document_data,
            'extracted_data': extracted_data
        }
        
    except Exception as e:
        logger.error(f"Error al recuperar datos de documento {document_id}: {str(e)}")
        logger.error(traceback.format_exc())
        return None

def process_financial_data(document_id, extracted_data):
    """
    Procesa los datos financieros ya extraídos y estructura la información.
    Esta función no llama a servicios externos como Textract o Comprehend.
    """
    try:
        start_time = time.time()
        logger.info(f"Procesando datos financieros para documento {document_id}")
        
        # Verificar si tenemos el texto completo
        if not extracted_data.get('texto_completo'):
            logger.warning(f"No se encontró texto completo para documento {document_id}")
            return {
                'success': False,
                'error': 'No hay texto completo disponible para procesar'
            }
        
        # Usar el texto completo ya extraído
        full_text = extracted_data['texto_completo']
        
        # Organizar entidades detectadas por tipo (si están disponibles)
        entities = {}
        if extracted_data.get('entidades'):
            if isinstance(extracted_data['entidades'], dict):
                # Si ya están organizadas por tipo
                entities = extracted_data['entidades']
            elif isinstance(extracted_data['entidades'], list):
                # Si es una lista de entidades, organizarlas por tipo
                for entity in extracted_data['entidades']:
                    if isinstance(entity, dict) and 'Type' in entity and 'Text' in entity:
                        entity_type = entity['Type']
                        entity_text = entity['Text']
                        
                        if entity_type not in entities:
                            entities[entity_type] = []
                        
                        if entity_text not in entities[entity_type]:
                            entities[entity_type].append(entity_text)
        
        # Extraer datos financieros usando el parser
        financial_data = parse_financial_document(full_text, entities)
        
        # Extraer transacciones desde el texto
        transactions = extract_transactions(full_text)
        if transactions:
            financial_data['movimientos'] = transactions
        elif 'tables' in extracted_data.get('metadatos_extraccion', {}):
            # Si hay tablas disponibles, intentar extraer transacciones de ellas
            tables = extracted_data['metadatos_extraccion']['tables']
            # Esta función necesitaría adaptarse para trabajar con la estructura de tablas
            # en metadatos_extraccion, pero depende de cómo se hayan almacenado
            # Simplemente pasamos las transacciones ya extraídas por textract_callback
            if 'transacciones' in extracted_data:
                financial_data['movimientos'] = extracted_data['transacciones']
        
        # Verificar si ya tenemos información específica extraída
        if extracted_data.get('specific_data'):
            # Si ya tenemos datos específicos, complementamos
            for key, value in extracted_data['specific_data'].items():
                if not financial_data.get(key) and value:
                    financial_data[key] = value
        
        # Añadir metadatos de procesamiento
        financial_data['fecha_procesamiento'] = datetime.now().isoformat()
        financial_data['tiempo_procesamiento'] = time.time() - start_time
        financial_data['fuente'] = 'financial_processor_optimizado'
        
        logger.info(f"Procesamiento financiero completado en {financial_data['tiempo_procesamiento']:.2f} segundos.")
        
        return {
            'success': True,
            'financial_data': financial_data
        }
        
    except Exception as e:
        logger.error(f"Error al procesar datos financieros {document_id}: {str(e)}")
        logger.error(traceback.format_exc())
        return {
            'success': False,
            'error': str(e)
        }

def validate_financial_data(financial_data):
    """
    Valida los datos financieros extraídos para determinar confianza y completitud
    """
    validation = {
        'is_valid': True,
        'confidence': 0.5,  # Base de confianza
        'errors': [],
        'warnings': []
    }
    
    # Verificar información básica
    if not financial_data.get('tipo_documento'):
        validation['warnings'].append("No se pudo determinar el tipo de documento financiero")
    
    # Para extractos bancarios, verificar número de cuenta y saldo
    if financial_data.get('tipo_documento') == 'extracto_bancario':
        if not financial_data.get('numero_cuenta'):
            validation['warnings'].append("No se ha podido extraer el número de cuenta")
        
        if not financial_data.get('saldo'):
            validation['warnings'].append("No se ha podido extraer el saldo")
        
        # Verificar si hay movimientos
        if not financial_data.get('movimientos') or len(financial_data.get('movimientos', [])) == 0:
            validation['warnings'].append("No se han podido extraer movimientos bancarios")
    
    # Para facturas, verificar importe total
    elif financial_data.get('tipo_documento') == 'factura':
        if not financial_data.get('importe_total'):
            validation['warnings'].append("No se ha podido extraer el importe total de la factura")
    
    # Para nóminas, verificar datos críticos
    elif financial_data.get('tipo_documento') == 'nomina':
        if not financial_data.get('salario_neto'):
            validation['warnings'].append("No se ha podido extraer el salario neto")
    
    # Calcular confianza basada en campos extraídos
    # La lista de campos cambia según el tipo de documento
    required_fields = ['tipo_documento']
    
    if financial_data.get('tipo_documento') == 'extracto_bancario':
        required_fields.extend(['numero_cuenta', 'saldo', 'fecha_extracto'])
    elif financial_data.get('tipo_documento') == 'factura':
        required_fields.extend(['importe_total', 'fecha_factura', 'emisor'])
    elif financial_data.get('tipo_documento') == 'nomina':
        required_fields.extend(['salario_neto', 'periodo', 'empresa'])
    
    # Contar campos completos
    complete_fields = sum(1 for field in required_fields if financial_data.get(field))
    base_confidence = complete_fields / len(required_fields) if required_fields else 0.5
    
    # Ajustar confianza por la presencia de movimientos
    if financial_data.get('tipo_documento') == 'extracto_bancario' and financial_data.get('movimientos'):
        # Si hay más de 5 movimientos, aumenta la confianza
        movement_bonus = min(0.2, len(financial_data.get('movimientos', [])) * 0.02)
        base_confidence += movement_bonus
    
    # Limitar confianza entre 0.3 y 0.95
    validation['confidence'] = min(0.95, max(0.3, base_confidence))
    
    # Si hay demasiadas advertencias, marcar con confianza baja pero no inválido
    if len(validation['warnings']) > 3:
        validation['confidence'] = max(0.3, validation['confidence'] - 0.2)
    
    return validation

def save_financial_data_to_db(document_id, financial_data, validation):
    """
    Guarda los datos financieros en la base de datos,
    gestionando errores y validando los datos.
    """
    try:
        # Verificar que financial_data sea JSON serializable
        try:
            json_data = json.dumps(financial_data)
        except (TypeError, OverflowError) as json_error:
            logger.error(f"Error al serializar datos financieros: {str(json_error)}")
            
            # Filtrar datos problemáticos
            cleaned_data = {}
            for key, value in financial_data.items():
                try:
                    json.dumps({key: value})
                    cleaned_data[key] = value
                except:
                    logger.warning(f"Campo no serializable: {key}, convertido a string")
                    if value is None:
                        cleaned_data[key] = None
                    else:
                        try:
                            cleaned_data[key] = str(value)
                        except:
                            cleaned_data[key] = f"ERROR: No serializable ({type(value).__name__})"
            
            # Usar datos limpios
            json_data = json.dumps(cleaned_data)
            financial_data = cleaned_data
        
        # Extraer valores para actualización
        confidence = validation.get('confidence', 0.0)
        is_valid = validation.get('is_valid', False)
        
        # Actualizar documento con datos extraídos
        update_document_extraction_data(
            document_id,
            json_data,
            confidence,
            is_valid
        )
        
        # Determinar estado de procesamiento
        status = 'completado' if is_valid else 'revisión_requerida'
        
        # Crear mensaje detallado
        message_parts = []
        if is_valid:
            message_parts.append(f"Documento financiero tipo {financial_data.get('tipo_documento', 'desconocido')} procesado correctamente")
        else:
            message_parts.append("Documento financiero procesado con advertencias")
            
        # Añadir información sobre validación
        if validation.get('warnings'):
            message_parts.append(f"{len(validation.get('warnings'))} advertencias encontradas")
        
        message = ". ".join(message_parts)
        
        # Añadir detalles completos
        details = {
            "validación": {
                "confianza": confidence,
                "es_válido": is_valid,
                "advertencias": validation.get('warnings', []),
                "errores": validation.get('errors', [])
            },
            "tipo_operación": "procesamiento_financiero",
            "campos_extraídos": list(financial_data.keys())
        }
        
        # Actualizar registro de análisis
        try:
            update_document_processing_status(
                document_id, 
                status, 
                json.dumps(details)
            )
            logger.info(f"Estado actualizado para documento {document_id}: {status}")
        except Exception as status_error:
            logger.error(f"Error al actualizar estado del documento: {str(status_error)}")
            
            # Intento de recuperación con nuevo registro
            try:
                analysis_id = generate_uuid()
                logger.info(f"Creando nuevo registro de análisis para {document_id}")
                
                # Insertar nuevo registro de análisis
                insert_analysis_record({
                    'id_analisis': analysis_id,
                    'id_documento': document_id,
                    'tipo_documento': financial_data.get('tipo_documento', 'extracto_bancario'),
                    'confianza_clasificacion': confidence,
                    'texto_extraido': None,  # No duplicamos el texto
                    'entidades_detectadas': json.dumps(financial_data.get('entidades_detectadas', {})),
                    'metadatos_extraccion': json.dumps({
                        'num_movimientos': len(financial_data.get('movimientos', [])),
                        'validation': details
                    }),
                    'fecha_analisis': datetime.now().isoformat(),
                    'estado_analisis': status,
                    'mensaje_error': None,
                    'version_modelo': 'financial_processor-1.0',
                    'tiempo_procesamiento': int(financial_data.get('tiempo_procesamiento', 0) * 1000),
                    'procesado_por': 'financial_processor',
                    'requiere_verificacion': not is_valid,
                    'verificado': False,
                    'verificado_por': None,
                    'fecha_verificacion': None
                })
                logger.info(f"Nuevo registro de análisis creado con éxito para {document_id}")
            except Exception as recovery_error:
                logger.error(f"Error de recuperación: {str(recovery_error)}")
                raise
        
        # Intentar vincular con cliente
        try:
            # Buscar posibles referencias de cliente en los datos
            client_info = None
            
            # Intentar por número de cuenta
            if financial_data.get('numero_cuenta'):
                if link_document_to_client(document_id, client_id=None, reference=financial_data['numero_cuenta']):
                    logger.info(f"Documento {document_id} vinculado a cliente mediante número de cuenta")
                    client_info = "Vinculado por número de cuenta"
            
            # Si no, intentar por titular
            if not client_info and financial_data.get('titular'):
                if link_document_to_client(document_id, client_id=None, name=financial_data['titular']):
                    logger.info(f"Documento {document_id} vinculado a cliente mediante nombre de titular")
                    client_info = "Vinculado por titular"
            
            # Último intento con document_id
            if not client_info:
                if link_document_to_client(document_id):
                    logger.info(f"Documento {document_id} vinculado a cliente")
                    client_info = "Vinculado por ID genérico"
            
            # Actualizar con información de vinculación
            if client_info:
                financial_data['cliente_info'] = client_info
                # Actualizar de nuevo para guardar esta información
                update_document_extraction_data(
                    document_id,
                    json.dumps(financial_data),
                    confidence,
                    is_valid
                )
            
        except Exception as link_error:
            logger.warning(f"No se pudo vincular el documento a un cliente: {str(link_error)}")
        
        return {
            "success": True,
            "document_id": document_id,
            "status": status,
            "confidence": confidence,
            "is_valid": is_valid,
            "analysis_id": analysis_id  # ← IMPORTANTE
        }
        
    except Exception as e:
        logger.error(f"Error grave al guardar datos financieros: {str(e)}")
        logger.error(traceback.format_exc())
        
        # Intentar actualizar el estado con el error
        try:
            error_msg = f"Error al guardar datos: {str(e)}"
            update_document_processing_status(document_id, 'error_guardado', error_msg)
        except:
            logger.error("No se pudo actualizar el estado con el error")
            
        return {
            "success": False,
            "document_id": document_id,
            "error": str(e)
        }

def lambda_handler(event, context):
    """
    Función principal que procesa documentos financieros.
    Optimizada para trabajar con datos ya extraídos por textract_callback.
    Se activa por mensajes de la cola SQS de documentos financieros.
    """
    start_time = time.time()
    logger.info("Evento recibido: " + json.dumps(event))
    
    response = {
        'procesados': 0,
        'errores': 0,
        'detalles': []
    }

    for record in event['Records']:
        documento_detalle = {
            'documento_id': None,
            'estado': 'sin_procesar',
            'tiempo': 0
        }
        
        record_start = time.time()
        
        try:
            # Parsear el mensaje SQS
            message_body = json.loads(record['body'])
            document_id = message_body['document_id']
            documento_detalle['documento_id'] = document_id
            
            logger.info(f"Procesando documento financiero {document_id}")
            
            # Paso 1: Obtener datos ya extraídos de la base de datos
            document_data_result = get_extracted_data_from_db(document_id)
            
            if not document_data_result:
                logger.error(f"No se pudieron recuperar datos del documento {document_id}")
                documento_detalle['estado'] = 'error_recuperacion_datos'
                response['errores'] += 1
                continue
            
            # Paso 2: Procesar los datos financieros
            process_result = process_financial_data(
                document_id, 
                document_data_result['extracted_data']
            )
            
            if not process_result['success']:
                logger.error(f"Error al procesar datos financieros: {process_result.get('error')}")
                documento_detalle['estado'] = 'error_procesamiento'
                documento_detalle['error'] = process_result.get('error')
                response['errores'] += 1
                continue
            
            # Paso 3: Validar los datos financieros
            financial_data = process_result['financial_data']
            validation = validate_financial_data(financial_data)
            
            # Paso 4: Guardar datos procesados en la base de datos
            save_result = save_financial_data_to_db(
                document_id,
                financial_data,
                validation
            )
            
            if save_result['success']:
                logger.info(f"Datos financieros guardados correctamente para {document_id}")
                documento_detalle['estado'] = 'procesado_completo'
                documento_detalle['confianza'] = validation.get('confidence', 0.0)
                response['procesados'] += 1

                # Evaluar si requiere revisión manual
                requires_review = evaluate_confidence(
                    validation['confidence'],
                    document_type=financial_data.get('tipo_documento', 'extracto_bancario'),
                    validation_results=validation
                )

                if requires_review:
                    logger.warning(f"Documento {document_id} marcado para revisión manual.")
                    mark_for_manual_review(
                        document_id=document_id,
                        analysis_id=save_result.get('analysis_id'),
                        confidence=validation['confidence'],
                        document_type=financial_data.get('tipo_documento', 'extracto_bancario'),
                        validation_info=validation,
                        extracted_data=financial_data
                    )
            else:
                logger.error(f"Error al guardar datos financieros: {save_result.get('error')}")
                documento_detalle['estado'] = 'error_guardado'
                documento_detalle['error'] = save_result.get('error')
                response['errores'] += 1
                
        except json.JSONDecodeError as json_error:
            logger.error(f"Error al decodificar mensaje SQS: {str(json_error)}")
            documento_detalle['estado'] = 'error_formato_json'
            documento_detalle['error'] = str(json_error)
            response['errores'] += 1
        except KeyError as key_error:
            logger.error(f"Falta campo requerido en mensaje: {str(key_error)}")
            documento_detalle['estado'] = 'error_campo_faltante'
            documento_detalle['error'] = str(key_error) 
            response['errores'] += 1
        except Exception as e:
            logger.error(f"Error general al procesar mensaje: {str(e)}")
            logger.error(traceback.format_exc())
            documento_detalle['estado'] = 'error_general'
            documento_detalle['error'] = str(e)
            response['errores'] += 1
        finally:
            documento_detalle['tiempo'] = time.time() - record_start
            response['detalles'].append(documento_detalle)

    response['tiempo_total'] = time.time() - start_time
    response['total_registros'] = len(event['Records'])
    
    logger.info(f"Procesamiento completado: {response['procesados']} exitosos, {response['errores']} errores en {response['tiempo_total']:.2f} segundos")

    return {
        'statusCode': 200,
        'body': json.dumps(response)
    }
