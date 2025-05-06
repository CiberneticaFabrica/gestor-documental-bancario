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
    execute_query
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
    
    for record in event['Records']:
        document_id = None
        try:
            # Parsear el mensaje SQS
            message_body = json.loads(record['body'])
            document_id = message_body['document_id']
            
            logger.info(f"Clasificando documento {document_id}")
            
            # IMPORTANTE: Verificar si ya tenemos datos de extracción disponibles
            extraction_complete = message_body.get('extraction_complete', False)
            
            if extraction_complete:
                # Caso óptimo: los datos ya están extraídos, no invocamos Textract/Comprehend
                logger.info(f"Extracción ya completada para documento {document_id}, usando datos existentes")
                
                # Obtener datos de análisis de la base de datos
                analysis_data = get_analysis_data(document_id)
                
                if analysis_data:
                    logger.info(f"Datos de análisis encontrados en base de datos para documento {document_id}")
                    
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
                    
                    # Verificar si hay patrones específicos de documentos de identidad
                    id_check_result = check_for_specific_id_patterns(texto_extraido, entidades_detectadas)
                    
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
                            # Clasificar usando el texto extraído
                            new_classification = classify_document(texto_extraido, entidades_detectadas)
                            classification_results = new_classification
                            requires_verification = new_classification['confidence'] < 0.85
                        else:
                            # Determinar si requiere verificación manual
                            requires_verification = confidence < 0.85
                            
                            # Obtener información del tipo de documento
                            doc_type_info = get_document_type_by_id(tipo_documento)
                            
                            # Si no se encuentra el tipo, usar una clasificación genérica
                            if not doc_type_info:
                                logger.warning(f"No se encontró información para el tipo {tipo_documento}, usando clasificación genérica")
                                tipo_documento = 'contrato'
                                doc_type_id = 'e5f6g7h8-5678-9012-abcd-ef1234567890123'  # ID genérico para contratos
                            else:
                                doc_type_id = doc_type_info['id_tipo_documento']
                                
                            # Usar la clasificación de la base de datos
                            classification_results = {
                                'document_type': tipo_documento,
                                'document_type_id': doc_type_id,
                                'confidence': confidence
                            }
                else:
                    # No se encontraron datos, debemos usar la información del mensaje
                    logger.warning(f"No se encontraron datos de análisis para documento {document_id}, usando datos del mensaje")
                    
                    # Usar la información del message_body si está disponible
                    doc_type_info = message_body.get('doc_type_info', {})
                    classification_results = {
                        'document_type': doc_type_info.get('document_type', 'contrato'),
                        'document_type_id': doc_type_info.get('document_type_id', 'e5f6g7h8-5678-9012-abcd-ef1234567890123'),
                        'confidence': doc_type_info.get('confidence', 0.7)
                    }
                    requires_verification = True
            else:
                # Caso excepcional: debemos obtener los datos nosotros
                # Esto es un fallback y no debería ocurrir normalmente en la arquitectura optimizada
                logger.warning(f"Extracción no completada para documento {document_id}, usando clasificación por nombre")
                
                # Intentar clasificar por nombre de archivo si está disponible
                key = message_body.get('key', '')
                if key:
                    filename = os.path.basename(unquote_plus(key))
                    classification_results = classify_by_filename(filename)
                else:
                    # Sin datos suficientes, usar clasificación genérica
                    classification_results = {
                        'document_type': 'contrato',
                        'document_type_id': 'e5f6g7h8-5678-9012-abcd-ef1234567890123',
                        'confidence': 0.5
                    }
                
                requires_verification = True
            
            # Actualizar estado de procesamiento
            update_document_processing_status(
                document_id,
                'clasificacion_completada',
                f"Documento clasificado como {classification_results['document_type']} con confianza {classification_results['confidence']:.2f}"
            )
            
            # Determinar la siguiente cola de procesamiento
            next_queue_url = determine_processor_queue(classification_results['document_type'])
            
            if not next_queue_url:
                logger.warning(f"No se encontró cola para {classification_results['document_type']}, usando cola por defecto")
                next_queue_url = DEFAULT_PROCESSOR_QUEUE_URL
            
            # Enviar para procesamiento especializado
            logger.info(f"Enviando documento a procesador especializado: {classification_results['document_type']}")
            
            # Registro de tiempo para análisis de rendimiento
            processing_time = time.time() - start_time
            
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
            except Exception as sqs_error:
                logger.error(f"Error al enviar a cola de procesamiento: {str(sqs_error)}")
            
        except json.JSONDecodeError as json_error:
            logger.error(f"Error al decodificar mensaje SQS: {str(json_error)}")
            continue
        except KeyError as key_error:
            logger.error(f"Falta campo requerido en mensaje: {str(key_error)}")
            continue
        except Exception as e:
            logger.error(f"Error general al procesar mensaje: {str(e)}")
            logger.error(traceback.format_exc())
    
    # Tiempo total de procesamiento
    total_time = time.time() - start_time
    logger.info(f"Procesamiento completo en {total_time:.2f} segundos")
    
    return {
        'statusCode': 200,
        'body': json.dumps('Clasificación de documentos completada')
    }