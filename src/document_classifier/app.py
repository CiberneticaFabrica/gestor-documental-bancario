# src/document_classifier/app.py
import os
import json
import boto3
import logging
import sys
import time
import traceback
import re
from urllib.parse import unquote_plus
from botocore.exceptions import ClientError

# Configurar el logger
logger = logging.getLogger()
logger.setLevel(os.environ.get('LOG_LEVEL', 'INFO'))

# Añadir directorio local al path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
sys.path.append('/opt/python')
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Importar funciones del módulo classifier y comunes
from classifier import classify_document, classify_by_filename, detect_cedula_format, check_specific_id_patterns,DOCUMENT_TYPE_IDS
from common.db_connector import (
    get_document_type_by_id, 
    update_document_processing_status,
    execute_query,
    log_document_processing_start,
    log_document_processing_end
)

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
ID_PROCESSOR_QUEUE_URL = os.environ.get('ID_PROCESSOR_QUEUE_URL')
CONTRACT_PROCESSOR_QUEUE_URL = os.environ.get('CONTRACT_PROCESSOR_QUEUE_URL')
FINANCIAL_PROCESSOR_QUEUE_URL = os.environ.get('FINANCIAL_PROCESSOR_QUEUE_URL')
DEFAULT_PROCESSOR_QUEUE_URL = os.environ.get('DEFAULT_PROCESSOR_QUEUE_URL', ID_PROCESSOR_QUEUE_URL)

def determine_processor_queue(document_type):
    """Determina la cola SQS adecuada según el tipo de documento"""
    type_queue_map = {
        'dni': ID_PROCESSOR_QUEUE_URL,
        'pasaporte': ID_PROCESSOR_QUEUE_URL,
        'nomina': FINANCIAL_PROCESSOR_QUEUE_URL,
        'extracto': FINANCIAL_PROCESSOR_QUEUE_URL,
        'extracto_bancario': FINANCIAL_PROCESSOR_QUEUE_URL,
        'contrato': CONTRACT_PROCESSOR_QUEUE_URL,
        'contrato_cuenta': CONTRACT_PROCESSOR_QUEUE_URL,
        'factura': FINANCIAL_PROCESSOR_QUEUE_URL,
        'recibo': FINANCIAL_PROCESSOR_QUEUE_URL,
        'formulario_kyc': CONTRACT_PROCESSOR_QUEUE_URL,
        'impuesto': FINANCIAL_PROCESSOR_QUEUE_URL
    }
    
    # Buscar coincidencias parciales también
    for key, queue in type_queue_map.items():
        if document_type.lower() == key or document_type.lower().startswith(key):
            return queue
    
    # Si no hay coincidencia, usar la cola por defecto
    return DEFAULT_PROCESSOR_QUEUE_URL

def sqs_operation_with_retry(operation_func, max_retries=3, **kwargs):
    """Ejecuta una operación SQS con reintentos en caso de fallos"""
    retries = 0
    while retries < max_retries:
        try:
            result = operation_func(**kwargs)
            return result
        except Exception as e:
            retries += 1
            logger.warning(f"Reintento {retries}/{max_retries} después de error: {str(e)}")
            if retries >= max_retries:
                logger.error(f"Error después de {max_retries} reintentos: {str(e)}")
                raise
            time.sleep(0.5 * retries)  # Backoff exponencial

def get_analysis_data(document_id):
    """Obtiene los datos de análisis ya procesados de la base de datos"""
    query = """
    SELECT texto_extraido, entidades_detectadas, metadatos_extraccion, 
           confianza_clasificacion, tipo_documento, estado_analisis
    FROM analisis_documento_ia
    WHERE id_documento = %s
    ORDER BY fecha_analisis DESC
    LIMIT 1
    """
    
    results = execute_query(query, (document_id,))
    if results and len(results) > 0:
        return results[0]
    return None

def check_for_specific_id_patterns(text, entities_detected=None):
    """
    Verifica si hay patrones específicos de documentos de identidad
    que puedan haber sido mal clasificados.
    """
    # Verificar texto para patrones de cédula panameña
    panama_cedula_matches, other_id_matches = detect_cedula_format(text)
    panama_score, general_id_score = check_specific_id_patterns(text)
    
    # Patrones específicos que indican fuertemente un documento de identidad
    strong_id_patterns = [
        r'(?i)REPUBLICA DE PANAMA.*TRIBUNAL ELECTORAL',
        r'(?i)FECHA DE NACIMIENTO.*LUGAR DE NACIMIENTO',
        r'(?i)EXPEDIDA.*EXPIRA',
        r'(?i)SEXO.*TIPO DE SANGRE',
        r'(?i)(PASAPORTE|PASSPORT)\s+N[Oº]?\.?\s+[A-Z0-9]+',
        r'(?i)(DNI|CEDULA|ID CARD)\s+N[Oº]?\.?\s+\d+'
    ]
    
    is_id_document = False
    id_confidence = 0.0
    id_type = None
    
    # Verificar patrones fuertes
    for pattern in strong_id_patterns:
        if re.search(pattern, text):
            is_id_document = True
            id_confidence += 0.2  # Incrementar confianza por cada patrón encontrado
    
    # Verificar si hay números de cédula panameña en el texto
    if panama_cedula_matches:
        is_id_document = True
        id_confidence += 0.3
        id_type = 'dni'  # Cédula panameña es un tipo de DNI
        logger.info(f"Detectado número de cédula panameña: {panama_cedula_matches}")
    
    # Verificar si hay entidades detectadas con formato de cédula o identificación
    if entities_detected and isinstance(entities_detected, dict):
        # Verificar si hay teléfonos que en realidad son cédulas
        if 'phone' in entities_detected:
            for phone in entities_detected['phone']:
                if isinstance(phone, str) and re.match(r'\d{1,2}-\d{3,4}-\d{1,4}', phone):
                    is_id_document = True
                    id_confidence += 0.4
                    id_type = 'dni'
                    logger.info(f"Detectado número de cédula clasificado como teléfono: {phone}")
    
    # Si la puntuación acumulada es suficiente, consideramos que es un documento de identidad
    if is_id_document:
        # Determinar si es pasaporte o DNI/cédula
        if not id_type:
            # Si contiene patrones específicos de pasaporte
            if any(re.search(r'(?i)(PASSPORT|PASAPORTE)', text)):
                id_type = 'pasaporte'
            else:
                id_type = 'dni'  # Por defecto DNI/cédula
        
        # Limitar la confianza entre 0.7 y 0.95
        id_confidence = min(0.95, max(0.7, id_confidence))
        
        return {
            'is_id_document': True,
            'id_type': id_type,
            'confidence': id_confidence
        }
    
    return {
        'is_id_document': False
    }

def lambda_handler(event, context):
    """
    Función principal que clasifica documentos SIN procesamiento redundante.
    Se activa por mensajes de la cola SQS de clasificación.
    """
    start_time = time.time()
    logger.info(f"Evento recibido: {json.dumps(event)}")
    
    # Registrar inicio del procesamiento global
    lambda_registro_id = log_document_processing_start(
        'document_classifier',  # No tenemos ID de documento aún
        'clasificacion_documentos',
        datos_entrada={"records_count": len(event.get('Records', []))}
    )
    
    processed_count = 0
    error_count = 0
    
    for record in event['Records']:
        document_id = None
        record_start_time = time.time()
        
        try:
            # Parsear el mensaje SQS
            message_body = json.loads(record['body'])
            document_id = message_body['document_id']
            
            # Registrar inicio de procesamiento para este documento
            registro_id = log_document_processing_start(
                document_id,
                'clasificacion_documento',
                datos_entrada=message_body,
                analisis_id=lambda_registro_id
            )
            
            logger.info(f"Clasificando documento {document_id}")
            
            # IMPORTANTE: Verificar si ya tenemos datos de extracción disponibles
            extraction_complete = message_body.get('extraction_complete', False)
            
            if extraction_complete:
                # Caso óptimo: los datos ya están extraídos, no invocamos Textract/Comprehend
                logger.info(f"Extracción ya completada para documento {document_id}, usando datos existentes")
                
                # Registrar inicio de obtención de datos de análisis
                db_registro_id = log_document_processing_start(
                    document_id,
                    'obtener_datos_analisis',
                    datos_entrada={"extraction_complete": True},
                    analisis_id=registro_id
                )
                
                # Obtener datos de análisis de la base de datos
                analysis_data = get_analysis_data(document_id)
                
                if analysis_data:
                    logger.info(f"Datos de análisis encontrados en base de datos para documento {document_id}")
                    
                    # Finalizar registro de obtención de datos de análisis
                    log_document_processing_end(
                        db_registro_id,
                        estado='completado',
                        datos_procesados={"tipo_documento": analysis_data.get('tipo_documento', 'desconocido')}
                    )
                    
                    # Registrar inicio de procesamiento de texto y entidades
                    text_registro_id = log_document_processing_start(
                        document_id,
                        'procesar_texto_entidades',
                        datos_entrada={"texto_length": len(analysis_data.get('texto_extraido', '')) if analysis_data.get('texto_extraido') else 0},
                        analisis_id=registro_id
                    )
                    
                    # Extraer la información relevante
                    texto_extraido = analysis_data.get('texto_extraido', '')
                    entidades_detectadas = None
                    
                    # Intentar parsear las entidades detectadas
                    if 'entidades_detectadas' in analysis_data and analysis_data['entidades_detectadas']:
                        try:
                            if isinstance(analysis_data['entidades_detectadas'], str):
                                entidades_detectadas = json.loads(analysis_data['entidades_detectadas'])
                            else:
                                entidades_detectadas = analysis_data['entidades_detectadas']
                        except json.JSONDecodeError:
                            logger.warning(f"Error al decodificar entidades_detectadas: {analysis_data['entidades_detectadas']}")
                    
                    log_document_processing_end(
                        text_registro_id,
                        estado='completado',
                        datos_procesados={"entidades_disponibles": entidades_detectadas is not None}
                    )
                    
                    # Registrar inicio de verificación de patrones específicos
                    patterns_registro_id = log_document_processing_start(
                        document_id,
                        'verificar_patrones_id',
                        datos_entrada={"texto_length": len(texto_extraido)},
                        analisis_id=registro_id
                    )
                    
                    # Verificar si hay patrones específicos de documentos de identidad
                    id_check_result = check_for_specific_id_patterns(texto_extraido, entidades_detectadas)
                    
                    log_document_processing_end(
                        patterns_registro_id,
                        estado='completado',
                        datos_procesados={"is_id_document": id_check_result.get('is_id_document', False)}
                    )
                    
                    # Registrar inicio de clasificación final
                    classify_registro_id = log_document_processing_start(
                        document_id,
                        'clasificacion_final',
                        datos_entrada={"id_check_result": id_check_result},
                        analisis_id=registro_id
                    )
                    
                    if id_check_result['is_id_document']:
                        # Si detectamos un documento de identidad con alta confianza, usamos esa clasificación
                        logger.info(f"Detectado documento de identidad específico: {id_check_result['id_type']}")
                        
                        classification_results = {
                            'document_type': id_check_result['id_type'],
                            'document_type_id': DOCUMENT_TYPE_IDS.get(id_check_result['id_type'], 
                                                                    DOCUMENT_TYPE_IDS['dni']),
                            'confidence': id_check_result['confidence']
                        }
                        requires_verification = False
                    else:
                        # Usar la clasificación original de la base de datos
                        tipo_documento = analysis_data.get('tipo_documento', 'desconocido')
                        confidence = float(analysis_data.get('confianza_clasificacion', 0.7))
                        
                        # Si el tipo está vacío o es desconocido, intentar clasificar con el texto extraído
                        if tipo_documento in ('', 'desconocido') or confidence < 0.6:
                            logger.info(f"Reclasificando documento con texto extraído debido a baja confianza o tipo desconocido")
                            
                            # Registrar inicio de reclasificación
                            reclass_registro_id = log_document_processing_start(
                                document_id,
                                'reclasificar_documento',
                                datos_entrada={"motivo": "baja_confianza" if confidence < 0.6 else "tipo_desconocido"},
                                analisis_id=classify_registro_id
                            )
                            
                            # Clasificar usando el texto extraído
                            new_classification = classify_document(texto_extraido, entidades_detectadas)
                            classification_results = new_classification
                            requires_verification = new_classification['confidence'] < 0.85
                            
                            log_document_processing_end(
                                reclass_registro_id,
                                estado='completado',
                                datos_procesados={
                                    "document_type": new_classification['document_type'],
                                    "confidence": new_classification['confidence']
                                }
                            )
                        else:
                            # Determinar si requiere verificación manual
                            requires_verification = confidence < 0.85
                            
                            # Obtener información del tipo de documento
                            doc_type_info_registro_id = log_document_processing_start(
                                document_id,
                                'obtener_info_tipo_documento',
                                datos_entrada={"tipo_documento": tipo_documento},
                                analisis_id=classify_registro_id
                            )
                            
                            doc_type_info = get_document_type_by_id(tipo_documento)
                            
                            # Si no se encuentra el tipo, usar una clasificación genérica
                            if not doc_type_info:
                                logger.warning(f"No se encontró información para el tipo {tipo_documento}, usando clasificación genérica")
                                tipo_documento = 'contrato'
                                doc_type_id = 'e5f6g7h8-5678-9012-abcd-ef1234567890123'  # ID genérico para contratos
                                
                                log_document_processing_end(
                                    doc_type_info_registro_id,
                                    estado='error',
                                    mensaje_error=f"No se encontró información para el tipo {tipo_documento}",
                                    datos_procesados={"tipo_generico": tipo_documento}
                                )
                            else:
                                doc_type_id = doc_type_info['id_tipo_documento']
                                
                                log_document_processing_end(
                                    doc_type_info_registro_id,
                                    estado='completado',
                                    datos_procesados={"id_tipo_documento": doc_type_id}
                                )
                                
                            # Usar la clasificación de la base de datos
                            classification_results = {
                                'document_type': tipo_documento,
                                'document_type_id': doc_type_id,
                                'confidence': confidence
                            }
                    
                    log_document_processing_end(
                        classify_registro_id,
                        estado='completado',
                        datos_procesados={
                            "document_type": classification_results['document_type'],
                            "confidence": classification_results['confidence'],
                            "requires_verification": requires_verification
                        }
                    )
                    
                else:
                    # No se encontraron datos, debemos usar la información del mensaje
                    logger.warning(f"No se encontraron datos de análisis para documento {document_id}, usando datos del mensaje")
                    
                    log_document_processing_end(
                        db_registro_id,
                        estado='error',
                        mensaje_error="No se encontraron datos de análisis",
                        datos_procesados={"fallback": "usando_datos_mensaje"}
                    )
                    
                    # Registrar inicio de clasificación por mensaje
                    fallback_registro_id = log_document_processing_start(
                        document_id,
                        'clasificacion_por_mensaje',
                        datos_entrada={"tiene_doc_type_info": 'doc_type_info' in message_body},
                        analisis_id=registro_id
                    )
                    
                    # Usar la información del message_body si está disponible
                    doc_type_info = message_body.get('doc_type_info', {})
                    classification_results = {
                        'document_type': doc_type_info.get('document_type', 'contrato'),
                        'document_type_id': doc_type_info.get('document_type_id', 'e5f6g7h8-5678-9012-abcd-ef1234567890123'),
                        'confidence': doc_type_info.get('confidence', 0.7)
                    }
                    requires_verification = True
                    
                    log_document_processing_end(
                        fallback_registro_id,
                        estado='completado',
                        datos_procesados={
                            "document_type": classification_results['document_type'],
                            "confidence": classification_results['confidence']
                        }
                    )
            else:
                # Caso excepcional: debemos obtener los datos nosotros
                # Esto es un fallback y no debería ocurrir normalmente en la arquitectura optimizada
                logger.warning(f"Extracción no completada para documento {document_id}, usando clasificación por nombre")
                
                fallback_registro_id = log_document_processing_start(
                    document_id,
                    'clasificacion_por_filename',
                    datos_entrada={"extraction_complete": False},
                    analisis_id=registro_id
                )
                
                # Intentar clasificar por nombre de archivo si está disponible
                key = message_body.get('key', '')
                if key:
                    filename = os.path.basename(unquote_plus(key))
                    classification_results = classify_by_filename(filename)
                    
                    log_document_processing_end(
                        fallback_registro_id,
                        estado='completado',
                        datos_procesados={
                            "filename": filename,
                            "document_type": classification_results['document_type'],
                            "confidence": classification_results['confidence']
                        }
                    )
                else:
                    # Sin datos suficientes, usar clasificación genérica
                    classification_results = {
                        'document_type': 'contrato',
                        'document_type_id': 'e5f6g7h8-5678-9012-abcd-ef1234567890123',
                        'confidence': 0.5
                    }
                    
                    log_document_processing_end(
                        fallback_registro_id,
                        estado='error',
                        mensaje_error="Sin nombre de archivo disponible",
                        datos_procesados={"clasificacion_generica": True}
                    )
                
                requires_verification = True
            
            # Actualizar estado de procesamiento
            update_status_registro_id = log_document_processing_start(
                document_id,
                'actualizar_estado_procesamiento',
                datos_entrada={
                    "document_type": classification_results['document_type'],
                    "confidence": classification_results['confidence']
                },
                analisis_id=registro_id
            )
            
            update_document_processing_status(
                document_id,
                'clasificacion_completada',
                f"Documento clasificado como {classification_results['document_type']} con confianza {classification_results['confidence']:.2f}"
            )
            
            log_document_processing_end(
                update_status_registro_id,
                estado='completado'
            )
            
            # Determinar la siguiente cola de procesamiento
            queue_registro_id = log_document_processing_start(
                document_id,
                'determinar_cola_procesamiento',
                datos_entrada={"document_type": classification_results['document_type']},
                analisis_id=registro_id
            )
            
            next_queue_url = determine_processor_queue(classification_results['document_type'])
            
            if not next_queue_url:
                logger.warning(f"No se encontró cola para {classification_results['document_type']}, usando cola por defecto")
                next_queue_url = DEFAULT_PROCESSOR_QUEUE_URL
                
                log_document_processing_end(
                    queue_registro_id,
                    estado='completado_con_advertencia',
                    mensaje_error=f"Cola no encontrada para tipo {classification_results['document_type']}",
                    datos_procesados={"queue_url": next_queue_url, "usando_default": True}
                )
            else:
                log_document_processing_end(
                    queue_registro_id,
                    estado='completado',
                    datos_procesados={"queue_url": next_queue_url}
                )
            
            # Enviar para procesamiento especializado
            logger.info(f"Enviando documento a procesador especializado: {classification_results['document_type']}")
            
            # Registro de tiempo para análisis de rendimiento
            processing_time = time.time() - record_start_time
            
            sqs_registro_id = log_document_processing_start(
                document_id,
                'enviar_a_cola_especializada',
                datos_entrada={
                    "queue_url": next_queue_url,
                    "document_type": classification_results['document_type']
                },
                analisis_id=registro_id
            )
            
            try:
                # Preparar mensaje para el procesador específico
                processor_message = {
                    'document_id': document_id,
                    'document_type': classification_results['document_type'],
                    'document_type_id': classification_results['document_type_id'],
                    'classification_confidence': classification_results['confidence'],
                    'requires_verification': requires_verification,
                    'processing_time_ms': int(processing_time * 1000),
                    'extraction_complete': True  # Indicar que los datos ya están extraídos
                }
                
                # Añadir campos del mensaje original si existen
                if 'bucket' in message_body:
                    processor_message['bucket'] = message_body['bucket']
                
                if 'key' in message_body:
                    processor_message['key'] = message_body['key']
                
                # Mantenemos datos adicionales si existen
                if 'specific_data_available' in message_body:
                    processor_message['specific_data_available'] = message_body['specific_data_available']
                
                # Enviar mensaje a la cola correspondiente
                sqs_operation_with_retry(
                    sqs_client.send_message,
                    QueueUrl=next_queue_url,
                    MessageBody=json.dumps(processor_message)
                )
                
                logger.info(f"Documento {document_id} clasificado como {classification_results['document_type']} " +
                           f"(confianza: {classification_results['confidence']:.2f}) " +
                           f"en {processing_time:.2f} segundos")
                
                log_document_processing_end(
                    sqs_registro_id,
                    estado='completado',
                    datos_procesados={"queue_url": next_queue_url}
                )
                
                # Registrar finalización exitosa del procesamiento de este documento
                log_document_processing_end(
                    registro_id,
                    estado='completado',
                    confianza=classification_results['confidence'],
                    datos_procesados={
                        "document_type": classification_results['document_type'],
                        "processing_time_seconds": processing_time
                    }
                )
                
                processed_count += 1
                
            except Exception as sqs_error:
                logger.error(f"Error al enviar a cola de procesamiento: {str(sqs_error)}")
                
                log_document_processing_end(
                    sqs_registro_id,
                    estado='error',
                    mensaje_error=str(sqs_error)
                )
                
                log_document_processing_end(
                    registro_id,
                    estado='error',
                    mensaje_error=f"Error al enviar a cola: {str(sqs_error)}",
                    datos_procesados={
                        "document_type": classification_results['document_type'],
                        "processing_time_seconds": processing_time
                    }
                )
                
                error_count += 1
            
        except json.JSONDecodeError as json_error:
            logger.error(f"Error al decodificar mensaje SQS: {str(json_error)}")
            
            if document_id:
                error_registro_id = log_document_processing_start(
                    document_id,
                    'error_decodificar_json',
                    datos_entrada={"mensaje": str(json_error)},
                    analisis_id=lambda_registro_id
                )
                
                log_document_processing_end(
                    error_registro_id,
                    estado='error',
                    mensaje_error=str(json_error)
                )
                
            error_count += 1
            continue
            
        except KeyError as key_error:
            logger.error(f"Falta campo requerido en mensaje: {str(key_error)}")
            
            if document_id:
                error_registro_id = log_document_processing_start(
                    document_id,
                    'error_campo_faltante',
                    datos_entrada={"campo": str(key_error)},
                    analisis_id=lambda_registro_id
                )
                
                log_document_processing_end(
                    error_registro_id,
                    estado='error',
                    mensaje_error=str(key_error)
                )
                
            error_count += 1
            continue
            
        except Exception as e:
            logger.error(f"Error general al procesar mensaje: {str(e)}")
            logger.error(traceback.format_exc())
            
            if document_id:
                error_registro_id = log_document_processing_start(
                    document_id,
                    'error_general',
                    datos_entrada={"tipo_error": type(e).__name__},
                    analisis_id=lambda_registro_id
                )
                
                log_document_processing_end(
                    error_registro_id,
                    estado='error',
                    mensaje_error=str(e)
                )
                
            error_count += 1
    
    # Tiempo total de procesamiento
    total_time = time.time() - start_time
    logger.info(f"Procesamiento completo en {total_time:.2f} segundos")
    
    # Registrar finalización del procesamiento global
    log_document_processing_end(
        lambda_registro_id,
        estado='completado',
        datos_procesados={
            "procesados": processed_count,
            "errores": error_count,
            "tiempo_total_segundos": total_time
        }
    )
    
    return {
        'statusCode': 200,
        'body': json.dumps('Clasificación de documentos completada')
    }