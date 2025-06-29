# src/processors/financial_processor/app.py - VERSIÓN SIMPLIFICADA
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
    link_document_to_client,
    assign_folder_and_link,
    get_client_id_by_document,
    log_document_processing_start,
    log_document_processing_end,
    execute_query
)
from financial_parser import parse_financial_document, extract_transactions

from common.flow_utilis import crear_instancia_flujo_documento


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

def get_extracted_data_from_db_simplified(document_id):
    """
    VERSIÓN SIMPLIFICADA: Recupera los datos ya extraídos por textract_callback
    SIN modificar el análisis existente - SOLO lectura
    """
    try:
        start_time = time.time()
        logger.info(f"📥 Recuperando datos extraídos para documento financiero {document_id}...")
        
        # Obtener documento básico
        document_data = get_document_by_id(document_id)
        if not document_data:
            logger.error(f"❌ No se encontró el documento {document_id}")
            return None
        
        # Obtener los datos extraídos del campo JSON
        extracted_data = {}
        if document_data.get('datos_extraidos_ia'):
            try:
                if isinstance(document_data['datos_extraidos_ia'], dict):
                    extracted_data = document_data['datos_extraidos_ia']
                else:
                    extracted_data = json.loads(document_data['datos_extraidos_ia'])
                logger.info(f"📄 Datos del documento procesados: {len(extracted_data)} campos")
            except json.JSONDecodeError:
                logger.error(f"❌ Error decodificando datos_extraidos_ia para documento {document_id}")
                return None
        
        # Obtener texto extraído y datos analizados
        query = """
        SELECT 
            id_analisis,
            texto_extraido, 
            entidades_detectadas, 
            metadatos_extraccion, 
            estado_analisis, 
            tipo_documento,
            confianza_clasificacion
        FROM analisis_documento_ia
        WHERE id_documento = %s
        ORDER BY fecha_analisis DESC
        LIMIT 1
        """
        
        analysis_results = execute_query(query, (document_id,))
        
        if not analysis_results:
            logger.warning(f"⚠️ No se encontró análisis en base de datos para documento {document_id}")
            # Continuar con lo que tengamos en datos_extraidos_ia
        else:
            analysis_data = analysis_results[0]
            logger.info(f"📊 Análisis encontrado: ID {analysis_data.get('id_analisis')}")
            
            # Agregar texto completo
            if analysis_data.get('texto_extraido'):
                extracted_data['texto_completo'] = analysis_data['texto_extraido']
            
            # Agregar entidades detectadas
            if analysis_data.get('entidades_detectadas'):
                try:
                    entidades = json.loads(analysis_data['entidades_detectadas']) if isinstance(analysis_data['entidades_detectadas'], str) else analysis_data['entidades_detectadas']
                    extracted_data['entidades'] = entidades
                except json.JSONDecodeError:
                    logger.warning(f"⚠️ Error al decodificar entidades_detectadas para documento {document_id}")
            
            # Agregar metadatos de extracción
            if analysis_data.get('metadatos_extraccion'):
                try:
                    metadatos = json.loads(analysis_data['metadatos_extraccion']) if isinstance(analysis_data['metadatos_extraccion'], str) else analysis_data['metadatos_extraccion']
                    extracted_data['metadatos_extraccion'] = metadatos
                except json.JSONDecodeError:
                    logger.warning(f"⚠️ Error al decodificar metadatos_extraccion para documento {document_id}")
            
            # Agregar tipo de documento detectado
            if analysis_data.get('tipo_documento'):
                extracted_data['tipo_documento_detectado'] = analysis_data['tipo_documento']
            
            # Agregar confianza
            if analysis_data.get('confianza_clasificacion'):
                extracted_data['confianza_inicial'] = analysis_data['confianza_clasificacion']
        
        # Registrar tiempo de consulta
        logger.info(f"✅ Datos recuperados para documento {document_id} en {time.time() - start_time:.2f} segundos")
        
        return {
            'document_id': document_id,
            'analysis_id': analysis_results[0].get('id_analisis') if analysis_results else None,
            'document_data': document_data,
            'extracted_data': extracted_data
        }
        
    except Exception as e:
        logger.error(f"❌ Error al recuperar datos de documento {document_id}: {str(e)}")
        logger.error(traceback.format_exc())
        return None

def process_financial_data_simplified(document_id, extracted_data):
    """
    VERSIÓN SIMPLIFICADA: Procesa los datos financieros ya extraídos
    Esta función no llama a servicios externos como Textract o Comprehend.
    """
    try:
        start_time = time.time()
        logger.info(f"🔍 Procesando datos financieros para documento {document_id}")
        
        # Verificar si tenemos el texto completo
        if not extracted_data.get('texto_completo'):
            logger.warning(f"⚠️ No se encontró texto completo para documento {document_id}")
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
                entities = extracted_data['entidades']
            elif isinstance(extracted_data['entidades'], list):
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
            if 'transacciones' in extracted_data:
                financial_data['movimientos'] = extracted_data['transacciones']
        
        # Verificar si ya tenemos información específica extraída
        if extracted_data.get('specific_data'):
            for key, value in extracted_data['specific_data'].items():
                if not financial_data.get(key) and value:
                    financial_data[key] = value
        
        # Añadir metadatos de procesamiento
        financial_data['fecha_procesamiento'] = datetime.now().isoformat()
        financial_data['tiempo_procesamiento'] = time.time() - start_time
        financial_data['fuente'] = 'financial_processor_simplificado'
        
        logger.info(f"✅ Procesamiento financiero completado en {financial_data['tiempo_procesamiento']:.2f} segundos.")
        
        return {
            'success': True,
            'financial_data': financial_data
        }
        
    except Exception as e:
        logger.error(f"❌ Error al procesar datos financieros {document_id}: {str(e)}")
        logger.error(traceback.format_exc())
        return {
            'success': False,
            'error': str(e)
        }

def validate_financial_data_simplified(financial_data):
    """
    VERSIÓN SIMPLIFICADA: Valida los datos financieros extraídos
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
        financial_data['tipo_documento'] = 'extracto_bancario'  # Default
    
    # Para extractos bancarios, verificar campos importantes
    if financial_data.get('tipo_documento') == 'extracto_bancario':
        if not financial_data.get('numero_cuenta'):
            validation['warnings'].append("No se pudo extraer el número de cuenta")
        
        if not financial_data.get('saldo'):
            validation['warnings'].append("No se pudo extraer el saldo")
        
        # Verificar si hay movimientos
        if not financial_data.get('movimientos') or len(financial_data.get('movimientos', [])) == 0:
            validation['warnings'].append("No se pudieron extraer movimientos bancarios")
    
    # Para facturas, verificar importe total
    elif financial_data.get('tipo_documento') == 'factura':
        if not financial_data.get('importe_total'):
            validation['warnings'].append("No se pudo extraer el importe total de la factura")
    
    # Para nóminas, verificar datos críticos
    elif financial_data.get('tipo_documento') == 'nomina':
        if not financial_data.get('salario_neto'):
            validation['warnings'].append("No se pudo extraer el salario neto")
    
    # Calcular confianza basada en campos extraídos
    required_fields = ['tipo_documento']
    
    if financial_data.get('tipo_documento') == 'extracto_bancario':
        required_fields.extend(['numero_cuenta', 'saldo'])
    elif financial_data.get('tipo_documento') == 'factura':
        required_fields.extend(['importe_total'])
    elif financial_data.get('tipo_documento') == 'nomina':
        required_fields.extend(['salario_neto'])
    
    # Contar campos completos
    complete_fields = sum(1 for field in required_fields if financial_data.get(field))
    base_confidence = complete_fields / len(required_fields) if required_fields else 0.5
    
    # Ajustar confianza por la presencia de movimientos
    if financial_data.get('tipo_documento') == 'extracto_bancario' and financial_data.get('movimientos'):
        movement_bonus = min(0.2, len(financial_data.get('movimientos', [])) * 0.02)
        base_confidence += movement_bonus
    
    # Limitar confianza entre 0.3 y 0.95
    validation['confidence'] = min(0.95, max(0.3, base_confidence))
    
    return validation

def evaluate_confidence_simple(confidence_score, validation_results):
    """Función simple para evaluar si requiere revisión manual"""
    requires_review = False
    
    # Requiere revisión si confianza es baja
    if confidence_score < 0.7:
        requires_review = True
    
    # Requiere revisión si hay errores críticos
    if validation_results.get('errors') and len(validation_results['errors']) > 0:
        requires_review = True
    
    # Requiere revisión si hay muchas advertencias
    if validation_results.get('warnings') and len(validation_results['warnings']) > 2:
        requires_review = True
    
    return requires_review

def lambda_handler(event, context):
    """
    VERSIÓN SIMPLIFICADA: Procesa documentos financieros sin modificar análisis existente
    - Lee datos del análisis existente
    - Guarda datos financieros en el documento
    - SIEMPRE asigna carpeta
    - NO modifica el análisis original
    """
    start_time = time.time()
    logger.info("="*80)
    logger.info("🚀 PROCESADOR FINANCIERO - VERSIÓN SIMPLIFICADA")
    logger.info("="*80)
    logger.info("Evento recibido: " + json.dumps(event))
    
    response = {
        'procesados': 0,
        'errores': 0,
        'requieren_revision': 0,
        'carpetas_asignadas': 0,
        'detalles': []
    }

    for record in event['Records']:
        documento_detalle = {
            'documento_id': None,
            'estado': 'sin_procesar',
            'tiempo': 0,
            'tipo_detectado': None,
            'datos_guardados': False,
            'carpeta_asignada': False,
            'requiere_revision': False
        }
        
        record_start = time.time()
        registro_id = None
        document_id = None
        
        try:
            # Parsear el mensaje SQS
            message_body = json.loads(record['body'])
            document_id = message_body['document_id']
            documento_detalle['documento_id'] = document_id
            
            logger.info(f"💰 Procesando documento financiero {document_id}")
            
            # Iniciar registro de procesamiento
            registro_id = log_document_processing_start(
                document_id, 
                'procesamiento_financiero_simplificado',
                datos_entrada=message_body
            )
            
            # ✅ PASO 1: Obtener datos extraídos de la BD (SIN MODIFICAR ANÁLISIS)
            logger.info(f"📥 Recuperando datos extraídos de la base de datos...")
            document_data_result = get_extracted_data_from_db_simplified(document_id)
            
            if not document_data_result:
                raise Exception(f"No se pudieron recuperar datos del documento {document_id}")
            
            # Obtener tipo detectado si existe
            tipo_detectado = document_data_result['extracted_data'].get('tipo_documento_detectado', 'extracto_bancario')
            documento_detalle['tipo_detectado'] = tipo_detectado
            
            # ✅ PASO 2: Procesar los datos financieros
            logger.info(f"🔍 Procesando datos financieros...")
            process_result = process_financial_data_simplified(
                document_id, 
                document_data_result['extracted_data']
            )
            
            if not process_result['success']:
                raise Exception(f"Error al procesar datos financieros: {process_result.get('error')}")
            
            financial_data = process_result['financial_data']
            
            # ✅ PASO 3: Validar los datos financieros
            validation = validate_financial_data_simplified(financial_data)
            
            logger.info(f"📊 Validación completada - Confianza: {validation['confidence']:.2f}")
            
            # ✅ PASO 4: Evaluar si requiere revisión manual
            requires_review = evaluate_confidence_simple(
                validation['confidence'],
                validation
            )
            
            documento_detalle['requiere_revision'] = requires_review
            
            if requires_review:
                response['requieren_revision'] += 1
                logger.warning(f"⚠️ Documento {document_id} requiere revisión manual")
            
            # ✅ PASO 5: Guardar datos financieros en el documento
            logger.info(f"💾 Guardando datos financieros procesados...")
            
            try:
                # Verificar que financial_data sea JSON serializable
                json_data = json.dumps(financial_data, ensure_ascii=False)
                
                # Actualizar documento con datos extraídos
                update_document_extraction_data(
                    document_id,
                    json_data,
                    validation['confidence'],
                    validation['is_valid']
                )
                
                logger.info(f"✅ Datos financieros guardados correctamente")
                documento_detalle['datos_guardados'] = True
                response['procesados'] += 1
                
            except Exception as save_error:
                logger.error(f"❌ Error al guardar datos financieros: {str(save_error)}")
                documento_detalle['estado'] = 'error_guardado'
                response['errores'] += 1
            
            # ✅ PASO 6: SIEMPRE ASIGNAR CARPETA (CRÍTICO SEGÚN REQUISITOS)
            logger.info(f"📁 Asignando carpeta para documento {document_id}...")
            
            try:
                # Buscar cliente del documento
                cliente_id = get_client_id_by_document(document_id)
                
                if cliente_id:
                    logger.info(f"👤 Cliente encontrado: {cliente_id}")
                    folder_result = assign_folder_and_link(cliente_id, document_id)
                    
                    if folder_result:
                        logger.info(f"✅ Carpeta asignada correctamente para documento {document_id}")
                        documento_detalle['carpeta_asignada'] = True
                        response['carpetas_asignadas'] += 1
                    else:
                        logger.warning(f"⚠️ No se pudo asignar carpeta para documento {document_id}")
                else:
                    logger.warning(f"⚠️ No se encontró cliente para documento {document_id}")
                    # Intentar vincular por datos financieros          
            except Exception as folder_error:
                logger.error(f"❌ Error asignando carpeta: {str(folder_error)}")
                # No fallar por error de carpeta, solo advertir
            
            # ✅ PASO 7: Actualizar estado del documento
            if requires_review:
                status = 'requiere_revision_manual'
                message = "Documento financiero procesado - Requiere revisión manual"
            elif validation['is_valid'] and documento_detalle['datos_guardados']:
                status = 'procesamiento_completado'
                message = "Documento financiero procesado y guardado correctamente"
            elif documento_detalle['datos_guardados']:
                status = 'requiere_revision_manual'
                message = "Documento financiero procesado con advertencias"
            else:
                status = 'requiere_revision_manual'
                message = "Documento financiero procesado pero no guardado"
            
            final_details = {
                'validación': validation,
                'tipo_documento': financial_data.get('tipo_documento'),
                'numero_cuenta': financial_data.get('numero_cuenta'),
                'saldo': financial_data.get('saldo'),
                'movimientos_count': len(financial_data.get('movimientos', [])),
                'campos_extraídos': [k for k, v in financial_data.items() if v is not None],
                'requires_review': requires_review,
                'datos_guardados': documento_detalle['datos_guardados'],
                'carpeta_asignada': documento_detalle['carpeta_asignada'],
                'procesador': 'financial_processor_simplificado'
            }
            
            update_document_processing_status(
                document_id, 
                status, 
                json.dumps(final_details, ensure_ascii=False)
            )
            
            documento_detalle['confianza'] = validation['confidence']
            documento_detalle['estado_final'] = status
            documento_detalle['estado'] = 'procesado'
            
            # Finalizar registro principal
            log_document_processing_end(
                registro_id, 
                estado='completado',
                confianza=validation['confidence'],
                datos_salida=final_details,
                mensaje_error=None if validation['is_valid'] else "Procesado con advertencias"
            )
            
            # ==================== PUBLICAR EVENTO ====================
            crear_instancia_flujo_documento(document_id)

            logger.info(f"✅ Documento {document_id} procesado completamente")
            logger.info(f"   💰 Tipo documento: {financial_data.get('tipo_documento')}")
            logger.info(f"   📊 Confianza: {validation['confidence']:.2f}")
            logger.info(f"   📝 Estado: {status}")
            logger.info(f"   📁 Carpeta asignada: {'Sí' if documento_detalle['carpeta_asignada'] else 'No'}")
            logger.info(f"   💾 Datos guardados: {'Sí' if documento_detalle['datos_guardados'] else 'No'}")
            logger.info(f"   🏦 Cuenta: {financial_data.get('numero_cuenta', 'No detectada')}")
            logger.info(f"   💵 Saldo: {financial_data.get('saldo', 'No detectado')}")
            logger.info(f"   📄 Movimientos: {len(financial_data.get('movimientos', []))}")
                
        except Exception as e:
            error_msg = str(e)
            logger.error(f"❌ Error procesando documento financiero {document_id if document_id else 'DESCONOCIDO'}: {error_msg}")
            logger.error(traceback.format_exc())
            
            documento_detalle['estado'] = 'error'
            documento_detalle['error'] = error_msg
            response['errores'] += 1
            
            # Actualizar estado de error
            if document_id:
                try:
                    update_document_processing_status(
                        document_id, 
                        'error_procesamiento_financiero',
                        f"Error en procesamiento financiero: {error_msg}"
                    )
                except:
                    pass
            
            # Finalizar registro con error
            if registro_id:
                log_document_processing_end(
                    registro_id, 
                    estado='error',
                    mensaje_error=error_msg
                )
                
        finally:
            # Calcular tiempo de procesamiento
            tiempo_procesamiento = time.time() - record_start
            documento_detalle['tiempo'] = tiempo_procesamiento
            response['detalles'].append(documento_detalle)

    # Resumen final
    total_time = time.time() - start_time
    response['tiempo_total'] = total_time
    response['total_registros'] = len(event['Records'])
    
    logger.info("="*80)
    logger.info("📊 RESUMEN DEL PROCESAMIENTO FINANCIERO")
    logger.info("="*80)
    logger.info(f"✅ Documentos procesados exitosamente: {response['procesados']}")
    logger.info(f"⚠️ Documentos que requieren revisión: {response['requieren_revision']}")
    logger.info(f"❌ Documentos con errores: {response['errores']}")
    logger.info(f"📁 Carpetas asignadas: {response['carpetas_asignadas']}")
    logger.info(f"⏱️ Tiempo total: {total_time:.2f} segundos")
    
    return {
        'statusCode': 200,
        'body': json.dumps(response, ensure_ascii=False)
    }