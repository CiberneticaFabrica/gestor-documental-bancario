# src/processors/contract_processor/app.py
import os
import json
import boto3
import logging
import sys
import time
from datetime import datetime
import traceback
import uuid
import re
from common.confidence_utils import evaluate_confidence, mark_for_manual_review

# Agregar las rutas para importar módulos comunes
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
sys.path.append('/opt')

from common.db_connector import (
    update_document_extraction_data,
    update_document_processing_status,
    get_document_by_id,
    insert_analysis_record,
    update_analysis_record,
    generate_uuid,
    link_document_to_client,
    assign_folder_and_link,
    get_client_id_by_document
)
 
from contract_parser import extract_contract_data, validate_contract_data, format_date, extract_clauses_by_section

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

# Instanciar clientes de AWS con reintentos
sqs_client = boto3.client('sqs', config=retry_config)

# Obtener variables de entorno
CONTRACT_PROCESSOR_QUEUE_URL = os.environ.get('CONTRACT_PROCESSOR_QUEUE_URL', '')

def operation_with_retry(operation_func, max_retries=3, base_delay=0.5, **kwargs):
    """Ejecuta una operación con reintentos en caso de fallos"""
    last_exception = None
    for attempt in range(max_retries):
        try:
            return operation_func(**kwargs)
        except Exception as e:
            last_exception = e
            delay = base_delay * (2 ** attempt)
            logger.warning(f"Reintento {attempt+1}/{max_retries} después de error: {str(e)}. Esperando {delay:.2f}s")
            time.sleep(delay)
    
    # Si llegamos aquí, todos los reintentos fallaron
    logger.error(f"Operación falló después de {max_retries} intentos. Último error: {str(last_exception)}")
    raise last_exception

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
        SELECT texto_extraido, entidades_detectadas, metadatos_extraccion, estado_analisis
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
            
            # Agregar texto completo y entidades detectadas a los datos extraídos
            if analysis_data.get('texto_extraido'):
                extracted_data['texto_completo'] = analysis_data['texto_extraido']
            
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

def process_contract_data(document_id, extracted_data):
    """
    Procesa los datos ya extraídos del contrato para estructurarlos según nuestro modelo.
    Esta función no llama a servicios externos como Textract.
    """
    try:
        start_time = time.time()
        logger.info(f"Procesando datos de contrato para documento {document_id}")
        
        # Verificar si tenemos el texto completo
        if not extracted_data.get('texto_completo'):
            logger.warning(f"No se encontró texto completo para documento {document_id}")
            return {
                'success': False,
                'error': 'No hay texto completo disponible para procesar'
            }
        
        # Usar el texto completo ya extraído
        full_text = extracted_data['texto_completo']
        
        # Crear bloques de texto a partir del texto completo
        # Usamos saltos de línea como separadores
        text_blocks = full_text.split('\n')
        
        # Usar el parser de contratos para extraer datos estructurados
        contract_data = extract_contract_data(full_text, text_blocks)
        
        # Complementar con datos ya extraídos
        if extracted_data.get('specific_data'):
            # Si ya tenemos datos específicos, complementamos
            for key, value in extracted_data['specific_data'].items():
                if not contract_data.get(key) and value:
                    contract_data[key] = value
        
        # Añadir información de entidades si está disponible
        if extracted_data.get('entidades'):
            # Buscar entidades que puedan ser firmantes
            posibles_firmantes = []
            for entidad in extracted_data['entidades']:
                if isinstance(entidad, dict) and entidad.get('Type') == 'PERSON':
                    nombre = entidad.get('Text', '')
                    if nombre and nombre not in posibles_firmantes:
                        posibles_firmantes.append(nombre)
            
            # Si encontramos posibles firmantes, los añadimos a los existentes
            if posibles_firmantes:
                existing_firmantes = contract_data.get('firmantes', [])
                for firmante in posibles_firmantes:
                    if firmante not in existing_firmantes:
                        existing_firmantes.append(firmante)
                contract_data['firmantes'] = existing_firmantes
        
        # Extraer cláusulas importantes y términos
        clause_sections = extract_clauses_by_section(full_text)
        if clause_sections:
            contract_data['secciones_clausulas'] = clause_sections
        
        # Formatear fechas si están presentes
        for field in ['fecha_inicio', 'fecha_fin']:
            if contract_data.get(field):
                iso_date = format_date(contract_data[field])
                if iso_date:
                    contract_data[f"{field}_iso"] = iso_date
        
        # Validar los datos extraídos
        validation = validate_contract_data(contract_data)
        
        # Añadir metadatos de procesamiento
        contract_data['fuente'] = 'contract_processor'
        contract_data['tiempo_procesamiento'] = time.time() - start_time
        contract_data['fecha_procesamiento'] = datetime.now().isoformat()
        contract_data['confianza'] = validation['confidence']
        
        logger.info(f"Procesamiento de contrato completado en {contract_data['tiempo_procesamiento']:.2f} segundos. Confianza: {validation['confidence']:.2f}")
        
        return {
            'success': True,
            'contract_data': contract_data,
            'validation': validation
        }
        
    except Exception as e:
        logger.error(f"Error al procesar datos de contrato {document_id}: {str(e)}")
        logger.error(traceback.format_exc())
        return {
            'success': False,
            'error': str(e)
        }

def save_contract_data_to_db(document_id, contract_data, validation, operation_type="procesamiento_normal"):
    """
    Guarda los datos de contrato extraídos en la base de datos de forma segura,
    gestionando errores y validando los datos antes de almacenarlos.
    """
    try:
        analysis_id = None  # Se inicializa para devolverlo al final

        # Verificar que contract_data sea JSON serializable
        try:
            json_data = json.dumps(contract_data)
        except (TypeError, OverflowError) as json_error:
            logger.error(f"Error al serializar datos del contrato: {str(json_error)}")
            
            # Filtrar y limpiar los datos problemáticos
            cleaned_data = {}
            for key, value in contract_data.items():
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
            
            json_data = json.dumps(cleaned_data)
            contract_data = cleaned_data

        # Extraer valores para actualización
        confidence = validation.get('confidence', 0.0)
        is_valid = validation.get('is_valid', False)

        # Paso 1: Actualizar los datos extraídos en la tabla documentos
        update_document_extraction_data(
            document_id,
            json_data,
            confidence,
            is_valid
        )

        # Paso 2: Determinar estado
        status = 'completado' if is_valid else 'revisión_requerida'
        if not is_valid and len(validation.get('errors', [])) > 0:
            status = 'error_validacion'

        # Crear mensaje y detalles
        message_parts = []
        if is_valid:
            message_parts.append("Contrato bancario procesado correctamente")
        else:
            message_parts.append("Contrato procesado con advertencias")

        if validation.get('warnings'):
            message_parts.append(f"{len(validation.get('warnings'))} advertencias encontradas")
        if validation.get('errors'):
            message_parts.append(f"{len(validation.get('errors'))} errores críticos")

        message = ". ".join(message_parts)

        details = {
            "validación": {
                "confianza": confidence,
                "es_válido": is_valid,
                "advertencias": validation.get('warnings', []),
                "errores": validation.get('errors', [])
            },
            "tipo_operación": operation_type,
            "campos_extraídos": list(contract_data.keys())
        }

        try:
            update_document_processing_status(
                document_id, 
                status, 
                json.dumps(details)
            )
            logger.info(f"Estado actualizado para documento {document_id}: {status}")

            # Generar un nuevo ID de análisis aunque haya ido bien
            analysis_id = generate_uuid()
            insert_analysis_record({
                'id_analisis': analysis_id,
                'id_documento': document_id,
                'tipo_documento': 'contrato',
                'confianza_clasificacion': confidence,
                'texto_extraido': None,
                'entidades_detectadas': json.dumps(contract_data.get('firmantes', [])),
                'metadatos_extraccion': json.dumps(details),
                'fecha_analisis': datetime.now().isoformat(),
                'estado_analisis': status,
                'mensaje_error': None,
                'version_modelo': 'contract_processor-1.0',
                'tiempo_procesamiento': int(contract_data.get('tiempo_procesamiento', 0) * 1000),
                'procesado_por': 'contract_processor',
                'requiere_verificacion': not is_valid,
                'verificado': False,
                'verificado_por': None,
                'fecha_verificacion': None
            })

        except Exception as status_error:
            logger.error(f"Error al actualizar estado del documento: {str(status_error)}")

            try:
                analysis_id = generate_uuid()
                insert_analysis_record({
                    'id_analisis': analysis_id,
                    'id_documento': document_id,
                    'tipo_documento': 'contrato',
                    'confianza_clasificacion': confidence,
                    'texto_extraido': None,
                    'entidades_detectadas': json.dumps(contract_data.get('firmantes', [])),
                    'metadatos_extraccion': json.dumps(details),
                    'fecha_analisis': datetime.now().isoformat(),
                    'estado_analisis': status,
                    'mensaje_error': str(status_error),
                    'version_modelo': 'contract_processor-1.0',
                    'tiempo_procesamiento': int(contract_data.get('tiempo_procesamiento', 0) * 1000),
                    'procesado_por': 'contract_processor',
                    'requiere_verificacion': not is_valid,
                    'verificado': False,
                    'verificado_por': None,
                    'fecha_verificacion': None
                })
                logger.info(f"Nuevo registro de análisis creado con éxito para {document_id}")
            except Exception as recovery_error:
                logger.error(f"Error también en recuperación: {str(recovery_error)}")
                raise

        # Paso 3: Intentar vincular el documento con un cliente
        #try:
        #    if contract_data.get('firmantes'):
        #        for firmante in contract_data['firmantes']:
        #            if link_document_to_client(document_id, client_id=None, firmante=firmante):
        #                logger.info(f"Documento {document_id} vinculado a cliente mediante firmante: {firmante}")
        #                break

        #    if contract_data.get('numero_contrato') or contract_data.get('numero_producto'):
        #        reference = contract_data.get('numero_contrato') or contract_data.get('numero_producto')
        #        if link_document_to_client(document_id, client_id=None, reference=reference):
        #            logger.info(f"Documento {document_id} vinculado a cliente mediante referencia: {reference}")

        #    link_document_to_client(document_id)

        #except Exception as link_error:
        #    logger.warning(f"No se pudo vincular el documento a un cliente: {str(link_error)}")

        return {
            "success": True,
            "document_id": document_id,
            "status": status,
            "confidence": confidence,
            "is_valid": is_valid,
            "analysis_id": analysis_id  # ← ¡Aquí está!
        }

    except Exception as e:
        logger.error(f"Error grave al guardar datos del contrato: {str(e)}")
        logger.error(traceback.format_exc())
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
    Función principal que procesa contratos bancarios.
    Optimizada para trabajar con datos ya extraídos por textract_callback.
    Se activa por mensajes de la cola SQS de contratos.
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
            message_body = json.loads(record['body'])
            document_id = message_body['document_id']
            documento_detalle['documento_id'] = document_id
            
            logger.info(f"Procesando contrato {document_id}")
            
     
            
            document_data_result = get_extracted_data_from_db(document_id)
            
            # Obtener tipo_documento si existe
            tipo_detectado = document_data_result['extracted_data'].get('tipo_documento_detectado', 'contrato')

            # Asignar carpeta y marcar documento solicitado como recibido
            cliente_id = get_client_id_by_document(document_id)
            assign_folder_and_link(cliente_id,document_id)
            logger.info(f"Procesando contrato Asignar carpeta y marcar documento solicitado como recibido {document_id}")

            if not document_data_result:
                logger.error(f"No se pudieron recuperar datos del documento {document_id}")
                documento_detalle['estado'] = 'error_recuperacion_datos'
                response['errores'] += 1
                continue
            
            process_result = process_contract_data(
                document_id, 
                document_data_result['extracted_data']
            )
            
            if not process_result['success']:
                logger.error(f"Error al procesar datos de contrato: {process_result.get('error')}")
                documento_detalle['estado'] = 'error_procesamiento'
                documento_detalle['error'] = process_result.get('error')
                response['errores'] += 1
                continue
            
            contract_data = process_result['contract_data']
            validation = process_result['validation']
            
            save_result = save_contract_data_to_db(
                document_id,
                contract_data,
                validation,
                'procesamiento_optimizado'
            )
            
            if save_result['success']:
                logger.info(f"Datos de contrato guardados correctamente para {document_id}")
                documento_detalle['estado'] = 'procesado_completo'
                documento_detalle['confianza'] = validation.get('confidence', 0.0)
                response['procesados'] += 1

                # ⚠️ Evaluar si requiere revisión manual
                requires_review = evaluate_confidence(
                    validation['confidence'],
                    document_type='contrato',
                    validation_results=validation
                )

                if requires_review:
                    logger.warning(f"Documento {document_id} marcado para revisión manual.")
                    mark_for_manual_review(
                        document_id=document_id,
                        analysis_id=save_result.get('analysis_id'),
                        confidence=validation['confidence'],
                        document_type='contrato',
                        validation_info=validation,
                        extracted_data=contract_data
                    )
            else:
                logger.error(f"Error al guardar datos de contrato: {save_result.get('error')}")
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

    total_time = time.time() - start_time
    response['tiempo_total'] = total_time
    response['total_registros'] = len(event['Records'])

    logger.info(f"Procesamiento completado: {response['procesados']} exitosos, {response['errores']} errores en {total_time:.2f} segundos")

    return {
        'statusCode': 200,
        'body': json.dumps(response)
    }
 