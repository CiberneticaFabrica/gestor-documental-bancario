# src/upload_processor/app.py
import os
import json
import boto3
import logging
import uuid
import time
import sys
from datetime import datetime
import traceback
from urllib.parse import unquote_plus
from textract_queries import get_queries_for_document_type, get_loan_contract_queries, get_account_contract_queries, get_id_document_queries, get_generic_contract_queries

# Configurar el logger
logger = logging.getLogger()
logger.setLevel(os.environ.get('LOG_LEVEL', 'INFO'))

# Agregar las rutas para importar módulos comunes
sys.path.append('/opt/python')
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Importar configuración con reintentos para servicios AWS
from botocore.config import Config
retry_config = Config(
    retries={
        'max_attempts': 3,
        'mode': 'standard'
    }
)

# Inicializar clientes AWS
s3_client = boto3.client('s3', config=retry_config)
sqs_client = boto3.client('sqs', config=retry_config)
textract_client = boto3.client('textract', config=retry_config)

# Obtener variables de entorno
CLASSIFICATION_QUEUE_URL = os.environ.get('CLASSIFICATION_QUEUE_URL')
PROCESSED_BUCKET = os.environ.get('PROCESSED_BUCKET')
TEXTRACT_SNS_TOPIC = os.environ.get('TEXTRACT_SNS_TOPIC', '')
TEXTRACT_ROLE_ARN = os.environ.get('TEXTRACT_ROLE_ARN', '')

# Importar funciones necesarias
try:
    from common.db_connector import (
        insert_document, insert_document_version, insert_analysis_record, 
        generate_uuid, update_document_processing_status, get_document_type_by_id,
        get_banking_doc_category, insert_audit_record, update_analysis_record,
        link_document_to_client,get_document_by_id,execute_query,log_document_processing_start,
        log_document_processing_end,preserve_document_data_before_update,verify_document_version_integrity,
        get_document_version_history,restore_document_version,compare_document_versions,
        get_latest_extraction_data,insert_migrated_document_info,get_version_id,update_document_extraction_data
    )
    from common.pdf_utils import (
        extract_text_with_pypdf2,
        extract_structured_patterns_pypdf2,
        extract_type_specific_data_pypdf2, 
        calculate_pypdf2_confidence,
        normalize_date_safe 
    )
    from common.s3_utils import get_file_metadata, copy_s3_object, delete_s3_object
    from common.validation import validate_document, determine_document_type, get_document_expiry_info
 
    from document_validator import determine_document_type_from_metadata
    logger.info("Módulos importados correctamente")
except ImportError as e:
    logger.error(f"Error importando módulos: {str(e)}")
    # Función de fallback en caso de problemas de importación
    def generate_uuid():
        return str(uuid.uuid4())

def s3_operation_with_retry(operation_func, max_retries=3, **kwargs):
    """Ejecuta una operación S3 con reintentos en caso de fallos"""
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

def check_s3_object_exists(bucket, key):
    """Verifica si un objeto existe en S3"""
    try:
        s3_client.head_object(Bucket=bucket, Key=key)
        return True
    except s3_client.exceptions.ClientError as e:
        if e.response['Error']['Code'] == '404':
            logger.warning(f"El objeto {bucket}/{key} no existe")
            return False
        else:
            logger.error(f"Error al verificar objeto: {str(e)}")
            raise

# Opción 1: Función para obtener sourceIp de forma segura
def obtener_ip_origen(context):
    try:
        # Verificar si identity está disponible y tiene el atributo sourceIp
        if hasattr(context, 'identity') and hasattr(context.identity, 'sourceIp'):
            return context.identity.sourceIp
        # Verificar si clientContext está disponible
        elif hasattr(context, 'client_context') and context.client_context:
            if hasattr(context.client_context, 'client') and hasattr(context.client_context.client, 'sourceIp'):
                return context.client_context.client.sourceIp
        return '0.0.0.0'  # Valor predeterminado
    except Exception:
        return '0.0.0.0'  # Valor predeterminado en caso de error 

def should_use_textract(metadata, doc_type_info, preliminary_classification):
    """
    Determina si es necesario usar Textract o si podemos procesarlo localmente
    """
    # Si es un PDF simple, intentamos con PyPDF2
    if metadata['extension'].lower() == 'pdf':
        # Documentos DNI/pasaporte casi siempre necesitan OCR
        if preliminary_classification['doc_type'] in ['dni', 'pasaporte']:
            logger.info(f"Documento de identidad detectado, requiere Textract")
            return True
            
        # Para PDFs "simples" usamos PyPDF2
        if metadata['size'] < 2 * 1024 * 1024:  # < 2MB
            logger.info(f"PDF simple detectado, intentaremos con PyPDF2")
            return False
    
    # Imágenes siempre requieren Textract
    if metadata['extension'].lower() in ['jpg', 'jpeg', 'png', 'tiff']:
        logger.info(f"Imagen detectada, requiere Textract")
        return True
    
    # Archivos Office siempre requieren Textract
    if metadata['extension'].lower() in ['doc', 'docx', 'xls', 'xlsx']:
        logger.info(f"Documento Office detectado, requiere Textract")
        return True
    
    # Por defecto, si no estamos seguros, usamos Textract
    logger.info(f"Tipo de documento indeterminado, usando Textract por defecto")
    return True

def get_analysis_id_for_document(document_id, version_id=None):
    """
    Obtiene el analysis_id existente para un documento/versión
    """
    try:
        if version_id:
            query = """
            SELECT id_analisis FROM analisis_documento_ia 
            WHERE id_documento = %s AND id_version = %s
            ORDER BY fecha_analisis DESC 
            LIMIT 1
            """
            result = execute_query(query, (document_id, version_id))
        else:
            query = """
            SELECT id_analisis FROM analisis_documento_ia 
            WHERE id_documento = %s
            ORDER BY fecha_analisis DESC 
            LIMIT 1
            """
            result = execute_query(query, (document_id,))
        
        if result:
            return result[0]['id_analisis']
        return None
        
    except Exception as e:
        logger.error(f"Error obteniendo analysis_id: {str(e)}")
        return None

def create_analysis_record_for_version(document_id, version_id, analysis_id):
    """
    Crea un registro de análisis básico para una versión
    """
    try:
        analysis_data = {
            'id_analisis': analysis_id,
            'id_documento': document_id,
            'id_version': version_id,
            'tipo_documento': 'documento',
            'confianza_clasificacion': 0.5,
            'texto_extraido': None,
            'entidades_detectadas': None,
            'metadatos_extraccion': json.dumps({'created_for': 'pypdf2_processing'}),
            'fecha_analisis': datetime.now().isoformat(),
            'estado_analisis': 'iniciado',
            'mensaje_error': None,
            'version_modelo': 'pypdf2-basic',
            'tiempo_procesamiento': 0,
            'procesado_por': 'upload_processor',
            'requiere_verificacion': True,
            'verificado': False,
            'verificado_por': None,
            'fecha_verificacion': None
        }
        
        insert_analysis_record(analysis_data)
        logger.info(f"Registro de análisis creado: {analysis_id}")
        return True
        
    except Exception as e:
        logger.error(f"Error creando registro de análisis: {str(e)}")
        return False

def validate_document_version_consistency(document_id):
    """
    Valida la consistencia entre documento y versiones
    """
    try:
        # Obtener información del documento
        doc_query = """
        SELECT id_documento, version_actual 
        FROM documentos 
        WHERE id_documento = %s
        """
        doc_result = execute_query(doc_query, (document_id,))
        
        if not doc_result:
            return False, "Documento no encontrado"
        
        version_actual = doc_result[0]['version_actual']
        
        # Verificar que la versión actual existe
        version_query = """
        SELECT id_version 
        FROM versiones_documento 
        WHERE id_documento = %s AND numero_version = %s
        """
        version_result = execute_query(version_query, (document_id, version_actual))
        
        if not version_result:
            return False, f"Versión actual {version_actual} no encontrada"
        
        return True, f"Consistencia validada para documento {document_id}, versión {version_actual}"
        
    except Exception as e:
        logger.error(f"Error validando consistencia: {str(e)}")
        return False, str(e)

#fin de funciones de pypdf2
def start_textract_processing(document_id, bucket, key, document_type_info=None, preliminary_classification=None):
    """
    VERSIÓN CORREGIDA: Configuración robusta de Textract con validación de queries
    """
    textract_registro_id = log_document_processing_start(
        document_id, 
        'iniciar_textract_v3',
        datos_entrada={
            "bucket": bucket, 
            "key": key, 
            "preliminary_classification": preliminary_classification
        }
    )
    
    try:
        # Verificar configuración requerida
        if not TEXTRACT_SNS_TOPIC or not TEXTRACT_ROLE_ARN:
            logger.warning("Variables de entorno para Textract asíncrono no configuradas")
            return send_to_classification_fallback(document_id, bucket, key, preliminary_classification)
        
        # Obtener información de versión
        doc_info = get_document_by_id(document_id)
        version_actual = doc_info.get('version_actual', 1) if doc_info else 1
        
        # Determinar tipo de documento para configuración
        doc_type = preliminary_classification.get('doc_type', 'documento') if preliminary_classification else 'documento'
        
        # ✅ CONFIGURACIÓN BASE SIEMPRE VÁLIDA
        feature_types = ['TABLES', 'FORMS']
        job_tag = f"{document_id}_v{version_actual}_textract"
        
        # ✅ CONFIGURACIÓN DE QUERIES MEJORADA CON VALIDACIÓN EXHAUSTIVA
        queries_config = None
        
        try:
            queries = None
            
            # Obtener queries según tipo de documento
            if doc_type in ['contrato', 'contrato_prestamo', 'prestamo_personal']:
                queries = get_loan_contract_queries()
                job_tag = f"{document_id}_v{version_actual}_loan_analysis"
            elif doc_type in ['contrato_cuenta', 'apertura_cuenta']:
                queries = get_account_contract_queries()
                job_tag = f"{document_id}_v{version_actual}_account_analysis"
            elif doc_type in ['dni', 'pasaporte', 'cedula']:
                queries = get_id_document_queries()
                job_tag = f"{document_id}_v{version_actual}_id_analysis"
            else:
                queries = get_generic_contract_queries()
                job_tag = f"{document_id}_v{version_actual}_generic_analysis"
            
            # ✅ VALIDACIÓN EXHAUSTIVA DE QUERIES
            if queries and isinstance(queries, list) and len(queries) > 0:
                valid_queries = []
                
                for i, query in enumerate(queries):
                    # Validar estructura básica
                    if not isinstance(query, dict):
                        logger.warning(f"⚠️ Query {i} no es un diccionario")
                        continue
                    
                    # Validar campos requeridos
                    if 'Text' not in query or 'Alias' not in query:
                        logger.warning(f"⚠️ Query {i} no tiene campos Text/Alias")
                        continue
                    
                    # Validar contenido
                    text = query['Text']
                    alias = query['Alias']
                    
                    if not isinstance(text, str) or not isinstance(alias, str):
                        logger.warning(f"⚠️ Query {i} Text/Alias no son strings")
                        continue
                    
                    if not text.strip() or not alias.strip():
                        logger.warning(f"⚠️ Query {i} Text/Alias están vacíos")
                        continue
                    
                    # Validar longitud (límites de AWS Textract)
                    if len(text) > 200 or len(alias) > 100:
                        logger.warning(f"⚠️ Query {i} excede límites de longitud")
                        continue
                    
                    # Query válida
                    valid_queries.append({
                        'Text': text.strip(),
                        'Alias': alias.strip()
                    })
                
                # Solo usar queries si tenemos al menos una válida
                if valid_queries and len(valid_queries) > 0:
                    # Limitar número de queries (máximo de AWS Textract es 15)
                    if len(valid_queries) > 15:
                        logger.warning(f"⚠️ Limitando queries de {len(valid_queries)} a 15")
                        valid_queries = valid_queries[:15]
                    
                    feature_types.append('QUERIES')
                    queries_config = {'Queries': valid_queries}
                    
                    logger.info(f"✅ QueriesConfig configurado: {len(valid_queries)} queries válidas")
                    
                    # Debug: mostrar queries configuradas
                    for i, q in enumerate(valid_queries):
                        logger.debug(f"  Query {i+1}: {q['Alias']} = {q['Text'][:50]}...")
                        
                else:
                    logger.warning(f"⚠️ Queries disponibles pero ninguna válida para tipo: {doc_type}")
            else:
                logger.info(f"ℹ️ Sin queries configuradas para tipo: {doc_type}")
                
        except Exception as query_error:
            logger.error(f"❌ Error configurando queries: {str(query_error)}")
            import traceback
            logger.error(f"📍 Query error trace: {traceback.format_exc()}")
            # Continuar sin queries en caso de error
        
        # ✅ PREPARAR PARÁMETROS DE TEXTRACT CON VALIDACIÓN FINAL
        textract_params = {
            'DocumentLocation': {
                'S3Object': {
                    'Bucket': bucket,
                    'Name': key
                }
            },
            'FeatureTypes': feature_types,
            'JobTag': job_tag,
            'NotificationChannel': {
                'SNSTopicArn': TEXTRACT_SNS_TOPIC,
                'RoleArn': TEXTRACT_ROLE_ARN
            }
        }
        
        # ✅ AGREGAR QUERIES SOLO SI ESTÁN CONFIGURADAS Y VALIDADAS
        if queries_config:
            # Validación final antes de agregar
            try:
                # Verificar que QueriesConfig tiene la estructura correcta
                if 'Queries' in queries_config and isinstance(queries_config['Queries'], list):
                    if len(queries_config['Queries']) > 0:
                        # Validación final de cada query
                        final_queries = []
                        for q in queries_config['Queries']:
                            if (isinstance(q, dict) and 
                                'Text' in q and 'Alias' in q and
                                isinstance(q['Text'], str) and isinstance(q['Alias'], str) and
                                q['Text'].strip() and q['Alias'].strip()):
                                final_queries.append(q)
                        
                        if final_queries:
                            textract_params['QueriesConfig'] = {'Queries': final_queries}
                            logger.info(f"✅ QueriesConfig añadido con {len(final_queries)} queries finales")
                        else:
                            logger.warning("⚠️ Todas las queries fallaron validación final")
                    else:
                        logger.warning("⚠️ Lista de queries vacía en validación final")
                else:
                    logger.warning("⚠️ Estructura de QueriesConfig inválida en validación final")
                    
            except Exception as final_validation_error:
                logger.error(f"❌ Error en validación final de queries: {str(final_validation_error)}")
                # Remover queries si hay problemas
                if 'QueriesConfig' in textract_params:
                    del textract_params['QueriesConfig']
                    feature_types.remove('QUERIES')
                    textract_params['FeatureTypes'] = feature_types
        
        # ✅ LOG DE CONFIGURACIÓN FINAL
        logger.info(f"🔍 Configuración final de Textract:")
        logger.info(f"   - Bucket: {bucket}")
        logger.info(f"   - Key: {key}")
        logger.info(f"   - FeatureTypes: {feature_types}")
        logger.info(f"   - JobTag: {job_tag}")
        logger.info(f"   - Queries: {'Sí (' + str(len(textract_params.get('QueriesConfig', {}).get('Queries', []))) + ')' if 'QueriesConfig' in textract_params else 'No'}")
        logger.info(f"   - SNS Topic: {TEXTRACT_SNS_TOPIC}")
        logger.info(f"   - Role ARN: {TEXTRACT_ROLE_ARN}")
        
        # ✅ INICIAR TEXTRACT CON MANEJO DE ERRORES MEJORADO
        try:
            logger.info(f"🚀 Iniciando Textract con configuración validada")
            textract_response = textract_client.start_document_analysis(**textract_params)
            
            job_id = textract_response['JobId']
            logger.info(f"✅ Textract iniciado exitosamente: JobId={job_id}")
            
            # Actualizar estado en BD
            metadata = {
                'textract_job_id': job_id,
                'textract_job_tag': job_tag,
                'feature_types': feature_types,
                'queries_count': len(textract_params.get('QueriesConfig', {}).get('Queries', [])),
                'doc_type': doc_type,
                'version_info': {
                    'version_actual': version_actual,
                    'processed_at': datetime.now().isoformat()
                },
                'bucket': bucket,
                'key': key
            }
            
            update_document_processing_status(
                document_id, 
                'textract_iniciado_v3',
                json.dumps(metadata),
                tipo_documento=doc_type
            )
            
            log_document_processing_end(
                textract_registro_id, 
                estado='completado',
                datos_procesados={
                    "job_id": job_id, 
                    "job_tag": job_tag,
                    "feature_types": feature_types,
                    "queries_count": len(textract_params.get('QueriesConfig', {}).get('Queries', []))
                }
            )
            
            return True
            
        except Exception as textract_error:
            error_message = str(textract_error)
            logger.error(f"❌ Error específico de Textract: {error_message}")
            
            # ✅ MANEJO MEJORADO DE ERRORES ESPECÍFICOS
            if "InvalidParameterException" in error_message:
                logger.error("🔧 Error de parámetros inválidos - Analizando causa:")
                
                # Verificar configuración específica
                config_issues = []
                
                if not TEXTRACT_SNS_TOPIC.startswith('arn:aws:sns:'):
                    config_issues.append(f"SNS Topic malformado: {TEXTRACT_SNS_TOPIC}")
                
                if not TEXTRACT_ROLE_ARN.startswith('arn:aws:iam:'):
                    config_issues.append(f"Role ARN malformado: {TEXTRACT_ROLE_ARN}")
                
                if 'QueriesConfig' in textract_params:
                    queries_count = len(textract_params['QueriesConfig']['Queries'])
                    if queries_count > 15:
                        config_issues.append(f"Demasiadas queries: {queries_count} (máximo 15)")
                
                if config_issues:
                    for issue in config_issues:
                        logger.error(f"   - {issue}")
                
                # ✅ FALLBACK INTELIGENTE: Intentar sin queries primero
                if 'QueriesConfig' in textract_params:
                    logger.info("🔧 Intentando fallback sin queries...")
                    
                    fallback_params = textract_params.copy()
                    del fallback_params['QueriesConfig']
                    fallback_params['FeatureTypes'] = ['TABLES', 'FORMS']
                    fallback_params['JobTag'] = f"{document_id}_v{version_actual}_fallback"
                    
                    try:
                        textract_response = textract_client.start_document_analysis(**fallback_params)
                        job_id = textract_response['JobId']
                        logger.info(f"✅ Textract fallback sin queries exitoso: JobId={job_id}")
                        
                        # Actualizar con configuración fallback
                        fallback_metadata = {
                            'textract_job_id': job_id,
                            'textract_job_tag': fallback_params['JobTag'],
                            'feature_types': ['TABLES', 'FORMS'],
                            'queries_count': 0,
                            'doc_type': doc_type,
                            'fallback_used': True,
                            'fallback_reason': 'queries_invalid_parameters',
                            'original_error': error_message
                        }
                        
                        update_document_processing_status(
                            document_id, 
                            'textract_iniciado_fallback_queries',
                            json.dumps(fallback_metadata),
                            tipo_documento=doc_type
                        )
                        
                        log_document_processing_end(
                            textract_registro_id, 
                            estado='completado',
                            datos_procesados={
                                "job_id": job_id, 
                                "fallback_used": True,
                                "fallback_reason": "queries_invalid_parameters",
                                "original_error": error_message
                            }
                        )
                        
                        return True
                        
                    except Exception as fallback_error:
                        logger.error(f"❌ Fallback sin queries también falló: {str(fallback_error)}")
                
                # ✅ SEGUNDO FALLBACK: Solo TEXT detection
                logger.info("🔧 Intentando segundo fallback con solo TEXT detection...")
                
                text_detection_params = {
                    'DocumentLocation': {
                        'S3Object': {
                            'Bucket': bucket,
                            'Name': key
                        }
                    },
                    'JobTag': f"{document_id}_v{version_actual}_text_only",
                    'NotificationChannel': {
                        'SNSTopicArn': TEXTRACT_SNS_TOPIC,
                        'RoleArn': TEXTRACT_ROLE_ARN
                    }
                }
                
                try:
                    textract_response = textract_client.start_document_text_detection(**text_detection_params)
                    job_id = textract_response['JobId']
                    logger.info(f"✅ Textract TEXT detection fallback exitoso: JobId={job_id}")
                    
                    # Actualizar con configuración de texto solamente
                    text_metadata = {
                        'textract_job_id': job_id,
                        'textract_job_tag': text_detection_params['JobTag'],
                        'feature_types': ['TEXT'],
                        'queries_count': 0,
                        'doc_type': doc_type,
                        'fallback_used': True,
                        'fallback_reason': 'analysis_invalid_parameters',
                        'fallback_level': 'text_detection_only',
                        'original_error': error_message
                    }
                    
                    update_document_processing_status(
                        document_id, 
                        'textract_iniciado_fallback_text',
                        json.dumps(text_metadata),
                        tipo_documento=doc_type
                    )
                    
                    log_document_processing_end(
                        textract_registro_id, 
                        estado='completado',
                        datos_procesados={
                            "job_id": job_id, 
                            "fallback_used": True,
                            "fallback_reason": "analysis_invalid_parameters",
                            "fallback_level": "text_detection_only"
                        }
                    )
                    
                    return True
                    
                except Exception as text_fallback_error:
                    logger.error(f"❌ Fallback de TEXT detection también falló: {str(text_fallback_error)}")
            
            elif "ThrottlingException" in error_message:
                logger.error("🔄 Textract está siendo limitado por rate limits")
                # Podríamos implementar retry con backoff aquí
                
            elif "LimitExceededException" in error_message:
                logger.error("📊 Se excedieron los límites de Textract")
                
            elif "AccessDenied" in error_message:
                logger.error("🔐 Error de permisos en Textract")
                logger.error(f"   - Verificar permisos del rol: {TEXTRACT_ROLE_ARN}")
                logger.error(f"   - Verificar acceso al bucket: {bucket}")
                
            # ✅ ÚLTIMO RECURSO: Fallback completo al procesamiento sin Textract
            logger.info("🔧 Todos los fallbacks de Textract fallaron, usando fallback completo...")
            raise textract_error
        
    except Exception as e:
        logger.error(f"❌ Error general al iniciar Textract: {str(e)}")
        
        log_document_processing_end(
            textract_registro_id, 
            estado='error',
            mensaje_error=str(e)
        )
        
        # ✅ FALLBACK FINAL: Enviar a clasificación sin Textract
        logger.info("📤 Enviando a procesamiento sin Textract como último recurso...")
        return send_to_classification_fallback(document_id, bucket, key, preliminary_classification)

def send_to_classification_without_textract(document_id, bucket, key, preliminary_classification):
    """
    🎯 VA EN UPLOAD_PROCESSOR
    Fallback cuando Textract no funciona
    """
    try:
        message = {
            'document_id': document_id,
            'bucket': bucket,
            'key': key,
            'skip_textract': True,
            'preliminary_classification': preliminary_classification
        }
        
        sqs_client.send_message(
            QueueUrl=CLASSIFICATION_QUEUE_URL,
            MessageBody=json.dumps(message)
        )
        
        logger.info(f"📤 Documento enviado a clasificación (sin Textract)")
        return True
        
    except Exception as e:
        logger.error(f"❌ Error en fallback: {str(e)}")
        return False

def send_to_classification_fallback(document_id, bucket, key, preliminary_classification):
    """
    Función de fallback cuando Textract falla - envía a clasificación sin procesamiento OCR
    """
    try:
        message = {
            'document_id': document_id,
            'bucket': bucket,
            'key': key,
            'skip_textract': True,
            'preliminary_classification': preliminary_classification,
            'reason': 'textract_failed',
            'fallback_processing': True
        }
        
        sqs_client.send_message(
            QueueUrl=CLASSIFICATION_QUEUE_URL,
            MessageBody=json.dumps(message)
        )
        
        # Actualizar estado en BD
        update_document_processing_status(
            document_id, 
            'enviado_clasificacion_fallback',
            json.dumps({
                'reason': 'textract_failed',
                'classification_queue': CLASSIFICATION_QUEUE_URL,
                'preliminary_classification': preliminary_classification
            })
        )
        
        logger.info(f"📤 Documento {document_id} enviado a clasificación (fallback por error Textract)")
        return True
        
    except Exception as sqs_error:
        logger.error(f"❌ Error en fallback SQS: {str(sqs_error)}")
        
        # Último recurso: actualizar estado como error
        update_document_processing_status(
            document_id, 
            'error',
            f"Error en Textract y fallback SQS: {str(sqs_error)}"
        )
        
        return False

# PASO 2: Procesar los resultados de queries en el callback
def process_document_metadata(document_id, metadata, bucket, key):
    """
    VERSIÓN CORREGIDA: Genera IDs consistentes y los registra en BD ANTES del procesamiento
    """
    metadata_registro_id = log_document_processing_start(
        document_id, 
        'procesar_metadatos_v2',
        datos_entrada={"bucket": bucket, "key": key, "metadata_filename": metadata['filename']}
    )
    
    try:
        # 1. ✅ GENERAR IDs ÚNICOS AL INICIO
        version_id = generate_uuid()
        analysis_id = generate_uuid()
        logger.info(f"🆔 IDs generados - Version: {version_id}, Analysis: {analysis_id}")
        
        # 2. ✅ CLASIFICACIÓN PRELIMINAR
        preliminary_classification = determine_document_type_from_metadata(metadata, metadata['filename'])
        doc_type_id = preliminary_classification.get('doc_type_id')
        
        if not doc_type_id:
            doc_type_id = determine_document_type(metadata, metadata['filename'])
        
        doc_type_info = get_document_type_by_id(doc_type_id)
        
        # 3. ✅ PREPARAR DATOS DE DOCUMENTO
        prefijo = doc_type_info.get('prefijo_nomenclatura', 'DOC') if doc_type_info else 'DOC'
        timestamp = int(time.time())
        doc_code = f"{prefijo}-{timestamp}-{document_id[:8]}"
        
        # Obtener metadatos adicionales de S3
        client_id = None
        external_file_id = None
        try:
            s3_response = s3_client.head_object(Bucket=bucket, Key=key)
            client_id = s3_response.get('Metadata', {}).get('client-id')
            external_file_id = s3_response.get('Metadata', {}).get('external-file-id') 
        except Exception as e:
            logger.warning(f"No se pudieron obtener metadatos adicionales: {str(e)}")
        
        # 4. ✅ DATOS DE DOCUMENTO
        document_data = {
            'id_documento': document_id,
            'codigo_documento': doc_code,
            'id_tipo_documento': doc_type_id,
            'titulo': metadata['filename'],
            'descripcion': f"Documento cargado automáticamente para procesamiento",
            'version_actual': 1,  # ✅ Primera versión
            'fecha_creacion': datetime.now().isoformat(),
            'fecha_modificacion': datetime.now().isoformat(),
            'creado_por': '691d8c44-f524-48fd-b292-be9e31977711',
            'modificado_por': '691d8c44-f524-48fd-b292-be9e31977711',
            'id_carpeta': None,
            'estado': 'PENDIENTE_PROCESAMIENTO',
            'confianza_extraccion': preliminary_classification.get('confidence', 0.5),
            'validado_manualmente': False,
            'metadatos': json.dumps({
                'formato': metadata['extension'],
                'tamano': metadata['size'],
                'origen': 'carga_automatica',
                'preliminary_classification': preliminary_classification,
                'version_id': version_id,  # ✅ Referencia al version_id
                'analysis_id': analysis_id  # ✅ Referencia al analysis_id
            })
        }
        
        # 5. ✅ DATOS DE VERSIÓN CON ID ESPECÍFICO
        version_data = {
            'id_version': version_id,  # ✅ USAR ID GENERADO
            'id_documento': document_id,
            'numero_version': 1,
            'fecha_creacion': datetime.now().isoformat(),
            'creado_por': '691d8c44-f524-48fd-b292-be9e31977711',
            'comentario_version': 'Versión inicial',
            'tamano_bytes': metadata['size'],
            'hash_contenido': metadata['hash'],
            'ubicacion_almacenamiento_tipo': 's3',
            'ubicacion_almacenamiento_ruta': f"{bucket}/{key}",
            'ubicacion_almacenamiento_parametros': json.dumps({'region': os.environ.get('AWS_REGION', 'us-east-1')}),
            'nombre_original': metadata['filename'],
            'extension': metadata['extension'],
            'mime_type': metadata['content_type'],
            'estado_ocr': 'PENDIENTE'
        }
        
        # 6. ✅ DATOS DE ANÁLISIS CON IDs ESPECÍFICOS
        analysis_data = {
            'id_analisis': analysis_id,  # ✅ USAR ID GENERADO
            'id_documento': document_id,
            'id_version': version_id,    # ✅ CRÍTICO: Vincular a versión específica
            'tipo_documento': doc_type_info.get('nombre_tipo', 'documento') if doc_type_info else 'documento',
            'estado_analisis': 'iniciado',
            'fecha_analisis': datetime.now().isoformat(),
            'confianza_clasificacion': preliminary_classification.get('confidence', 0.5),
            'texto_extraido': None,
            'entidades_detectadas': None,
            'metadatos_extraccion': json.dumps({
                'preliminary_classification': preliminary_classification,
                'version_info': {
                    'version_id': version_id,
                    'numero_version': 1
                },
                'processing_stage': 'initial_metadata'
            }),
            'mensaje_error': None,
            'version_modelo': 'clasificacion-inicial',
            'tiempo_procesamiento': 0,
            'procesado_por': 'upload_processor_v2',
            'requiere_verificacion': True,
            'verificado': False,
            'verificado_por': None,
            'fecha_verificacion': None
        }
        
        # 7. ✅ INSERTAR EN BD EN ORDEN CORRECTO
        db_registro_id = log_document_processing_start(
            document_id, 
            'insertar_bd_completa',
            datos_entrada={
                "documento_id": document_id,
                "version_id": version_id,
                "analysis_id": analysis_id
            },
            analisis_id=metadata_registro_id
        )
        
        try:
            # Orden importante: documento -> versión -> análisis
            logger.info(f"📝 Insertando documento {document_id}")
            insert_document(document_data)
            
            logger.info(f"📄 Insertando versión {version_id}")
            insert_document_version(version_data)
            
            logger.info(f"🔍 Insertando análisis {analysis_id}")
            insert_analysis_record(analysis_data)
            
            # Vincular cliente si existe
            if client_id:
                link_result = link_document_to_client(document_id, client_id)
                logger.info(f"🔗 Documento vinculado a cliente {client_id}: {link_result}")
            
            # Registrar documento migrado si aplica
            if external_file_id and client_id:
                insert_migrated_document_info(
                    creatio_file_id=external_file_id,
                    id_documento=document_id,
                    id_cliente=client_id,
                    nombre_archivo=metadata['filename']
                )
                logger.info(f"📋 Documento migrado registrado: {external_file_id}")
            
            log_document_processing_end(
                db_registro_id, 
                estado='completado',
                datos_procesados={
                    "documento_insertado": True,
                    "version_insertada": True,
                    "analisis_insertado": True
                }
            )
            
            logger.info(f"✅ Documento {document_id} registrado completamente en BD")
            
            # 8. ✅ RETORNAR IDs PARA USO POSTERIOR
            result = {
                'analysis_id': analysis_id,        # ✅ Para usar en procesamiento
                'version_id': version_id,          # ✅ Para usar en procesamiento
                'document_type_id': doc_type_id,
                'document_type_info': doc_type_info,
                'preliminary_classification': preliminary_classification,
                'client_id': client_id
            }
            
            log_document_processing_end(
                metadata_registro_id, 
                estado='completado',
                datos_procesados=result
            )
            
            return result
            
        except Exception as db_error:
            logger.error(f"❌ Error al insertar en base de datos: {str(db_error)}")
            
            log_document_processing_end(
                db_registro_id, 
                estado='error',
                mensaje_error=str(db_error)
            )
            
            raise db_error
            
    except Exception as e:
        logger.error(f"❌ Error en process_document_metadata: {str(e)}")
        
        log_document_processing_end(
            metadata_registro_id, 
            estado='error',
            mensaje_error=str(e)
        )
        
        raise e

def process_new_document(document_id, file_metadata, dest_bucket, dest_key, client_id=None):
    """
    VERSIÓN SIMPLIFICADA: Procesa un nuevo documento SOLO con Textract
    """
    process_doc_registro_id = log_document_processing_start(
        document_id,
        'procesar_nuevo_documento_textract_only',
        datos_entrada={
            "dest_bucket": dest_bucket,
            "dest_key": dest_key,
            "filename": file_metadata['filename'],
            "client_id": client_id
        }
    )

    try:
        # 1. ✅ PROCESAR METADATOS Y OBTENER IDs VERIFICADOS
        metadata_result = process_document_metadata(document_id, file_metadata, dest_bucket, dest_key)
        
        if not metadata_result or not metadata_result.get('analysis_id') or not metadata_result.get('version_id'):
            error_msg = "No se pudieron obtener IDs válidos del procesamiento de metadatos"
            logger.error(f"❌ {error_msg}")
            
            log_document_processing_end(
                process_doc_registro_id,
                estado='error',
                mensaje_error=error_msg
            )
            return False
        
        # Extraer IDs verificados
        analysis_id = metadata_result['analysis_id']
        version_id = metadata_result['version_id']
        document_type_info = metadata_result.get('document_type_info', {})
        preliminary_classification = metadata_result.get('preliminary_classification', {})
        
        logger.info(f"✅ IDs obtenidos - Analysis: {analysis_id}, Version: {version_id}")
        
        # 2. ✅ PROCESAR ÚNICAMENTE CON TEXTRACT
        logger.info(f"🔄 Iniciando procesamiento con Textract para documento {document_id}")
        
        textract_registro_id = log_document_processing_start(
            document_id,
            'iniciar_textract_exclusivo',
            datos_entrada={
                "version_id": version_id,
                "analysis_id": analysis_id,
                "doc_type": preliminary_classification.get('doc_type'),
                "processing_method": "textract_only"
            },
            analisis_id=process_doc_registro_id
        )
        
        textract_result = start_textract_processing(
            document_id,
            dest_bucket,
            dest_key,
            document_type_info,
            preliminary_classification
        )
        
        if not textract_result:
            error_msg = "Procesamiento con Textract falló"
            logger.error(f"❌ {error_msg}")
            
            log_document_processing_end(
                textract_registro_id,
                estado='error',
                mensaje_error=error_msg
            )
            
            log_document_processing_end(
                process_doc_registro_id,
                estado='error',
                mensaje_error=error_msg
            )
            return False
        
        log_document_processing_end(
            textract_registro_id,
            estado='completado',
            datos_procesados={
                "textract_started": True,
                "version_id": version_id,
                "analysis_id": analysis_id
            }
        )
        
        # 3. ✅ FINALIZAR PROCESO PRINCIPAL
        log_document_processing_end(
            process_doc_registro_id,
            estado='completado',
            datos_procesados={
                "processing_method": "textract_only",
                "analysis_id": analysis_id,
                "version_id": version_id,
                "textract_job_started": True
            }
        )
        
        logger.info(f"✅ Documento {document_id} enviado a Textract correctamente")
        logger.info(f"   📄 Version ID: {version_id}")
        logger.info(f"   🔍 Analysis ID: {analysis_id}")
        logger.info(f"   📋 Tipo detectado: {preliminary_classification.get('doc_type', 'desconocido')}")
        
      
        return True

    except Exception as e:
        logger.error(f"❌ Error al procesar nuevo documento con Textract: {str(e)}")
        import traceback
        logger.error(f"📍 Stack trace: {traceback.format_exc()}")

        log_document_processing_end(
            process_doc_registro_id,
            estado='error',
            mensaje_error=str(e)
        )

        return False
# Modificaciones para src/upload_processor/app.py
def process_new_document_version(document_id, file_metadata, dest_bucket, dest_key, client_id=None):
    """
    Procesa una nueva versión de un documento existente usando únicamente Textract.
    """
    version_registro_id = log_document_processing_start(
        document_id,
        'procesar_nueva_version_mejorada',
        datos_entrada={
            "dest_bucket": dest_bucket,
            "dest_key": dest_key,
            "filename": file_metadata['filename'],
            "client_id": client_id
        }
    )

    try:
        existing_document = get_document_by_id(document_id)
        if not existing_document:
            logger.error(f"No se encontró el documento {document_id} para actualizar versión")
            log_document_processing_end(version_registro_id, estado='error', mensaje_error=f"Documento {document_id} no encontrado")
            return process_new_document(document_id, file_metadata, dest_bucket, dest_key, client_id)

        logger.info(f"Documento existente encontrado: {document_id}")

        # Preservar datos extraídos anteriores
        if existing_document.get('datos_extraidos_ia'):
            preserve_document_data_before_update(
                document_id,
                f"Preservación automática (v{existing_document.get('version_actual')} -> v{existing_document.get('version_actual', 0) + 1})"
            )

        # Calcular nueva versión
        version_result = execute_query(
            "SELECT MAX(numero_version) as ultima_version FROM versiones_documento WHERE id_documento = %s",
            (document_id,)
        )
        current_version = version_result[0]['ultima_version'] if version_result and version_result[0]['ultima_version'] else 0
        new_version_number = current_version + 1
        version_id = generate_uuid()
        analysis_id = generate_uuid()

        # Insertar nueva versión
        version_data = {
            'id_version': version_id,
            'id_documento': document_id,
            'numero_version': new_version_number,
            'fecha_creacion': datetime.now().isoformat(),
            'creado_por': existing_document.get('modificado_por', 'auto'),
            'comentario_version': f'Nueva versión {new_version_number} - {file_metadata["filename"]}',
            'tamano_bytes': file_metadata['size'],
            'hash_contenido': file_metadata['hash'],
            'ubicacion_almacenamiento_tipo': 's3',
            'ubicacion_almacenamiento_ruta': f"{dest_bucket}/{dest_key}",
            'ubicacion_almacenamiento_parametros': json.dumps({'region': os.environ.get('AWS_REGION', 'us-east-1')}),
            'nombre_original': file_metadata['filename'],
            'extension': file_metadata['extension'],
            'mime_type': file_metadata['content_type'],
            'estado_ocr': 'PENDIENTE',
            'miniaturas_generadas': False
        }
        insert_document_version(version_data)

        # Actualizar documento principal
        execute_query(
            """
            UPDATE documentos
            SET version_actual = %s,
                fecha_modificacion = %s,
                modificado_por = %s,
                titulo = %s,
                estado = 'Pendiente_procesamiento'
            WHERE id_documento = %s
            """,
            (
                new_version_number,
                datetime.now().isoformat(),
                existing_document.get('modificado_por', 'auto'),
                file_metadata['filename'],
                document_id
            ),
            fetch=False
        )

        # Crear análisis
        insert_analysis_record({
            'id_analisis': analysis_id,
            'id_documento': document_id,
            'id_version': version_id,
            'tipo_documento': existing_document.get('nombre_tipo', 'documento'),
            'estado_analisis': 'iniciado',
            'fecha_analisis': datetime.now().isoformat(),
            'confianza_clasificacion': 0.5,
            'texto_extraido': None,
            'entidades_detectadas': None,
            'metadatos_extraccion': json.dumps({
                'version': new_version_number,
                'version_id': version_id,
                'filename': file_metadata['filename'],
                'created_for': 'nueva_version'
            }),
            'mensaje_error': None,
            'version_modelo': 'nueva-version',
            'tiempo_procesamiento': 0,
            'procesado_por': 'upload_processor',
            'requiere_verificacion': True,
            'verificado': False,
            'verificado_por': None,
            'fecha_verificacion': None
        })

        # Procesar directamente con Textract
        preliminary_classification = determine_document_type_from_metadata(file_metadata, file_metadata['filename'])

        textract_result = start_textract_processing(
            document_id,
            dest_bucket,
            dest_key,
            {'nombre_tipo': existing_document.get('nombre_tipo')},
            preliminary_classification
        )

        log_document_processing_end(
            version_registro_id,
            estado='completado' if textract_result else 'advertencia',
            datos_procesados={
                'nueva_version': new_version_number,
                'version_id': version_id,
                'analysis_id': analysis_id,
                'processing_method': 'textract',
                'processing_success': textract_result
            }
        )
 
        return True

    except Exception as e:
        logger.error(f"Error al procesar nueva versión de documento: {str(e)}")
        log_document_processing_end(version_registro_id, estado='error', mensaje_error=str(e))
        return process_new_document(document_id, file_metadata, dest_bucket, dest_key, client_id)


def lambda_handler(event, context):
    """
    Función principal que procesa la carga de documentos.
    Se activa cuando se carga un documento en el bucket de S3.
    """
    lambda_registro_id = log_document_processing_start(
        'no_document_id',  # No tenemos ID de documento todavía
        'upload_processor',
        datos_entrada={"records_count": len(event.get('Records', []))}
    )
    
    try:
        logger.info(f"Evento recibido: {json.dumps(event)}")
        
        processed_documents = 0
        error_documents = 0
        
        # Procesar cada registro del evento S3
        for record in event['Records']:
            # Obtener información del bucket y clave
            bucket = record['s3']['bucket']['name']
            
            # Decodificar la clave URL para manejar caracteres especiales
            key = unquote_plus(record['s3']['object']['key'])
            
            record_registro_id = log_document_processing_start(
                'record_' + str(processed_documents + error_documents),
                'procesar_registro_s3',
                datos_entrada={"bucket": bucket, "key": key},
                analisis_id=lambda_registro_id
            )
            
            logger.info(f"Procesando documento: {bucket}/{key}")
            
            try:
                # Verificar si el objeto existe antes de continuar
                exist_registro_id = log_document_processing_start(
                    'record_' + str(processed_documents + error_documents),
                    'verificar_objeto_s3',
                    datos_entrada={"bucket": bucket, "key": key},
                    analisis_id=record_registro_id
                )
                
                if not check_s3_object_exists(bucket, key):
                    logger.error(f"El objeto {bucket}/{key} no existe, saltando procesamiento")
                    
                    log_document_processing_end(
                        exist_registro_id, 
                        estado='error',
                        mensaje_error=f"Objeto {bucket}/{key} no existe"
                    )
                    
                    log_document_processing_end(
                        record_registro_id, 
                        estado='error',
                        mensaje_error=f"Objeto {bucket}/{key} no existe"
                    )
                    
                    error_documents += 1
                    continue
                
                log_document_processing_end(
                    exist_registro_id, 
                    estado='completado',
                    datos_procesados={"exists": True}
                )
                
                # Obtener metadatos de S3 para determinar si es una nueva versión
                metadata_registro_id = log_document_processing_start(
                    'record_' + str(processed_documents + error_documents),
                    'obtener_metadatos_s3',
                    datos_entrada={"bucket": bucket, "key": key},
                    analisis_id=record_registro_id
                )
                
                try:
                    s3_response = s3_client.head_object(Bucket=bucket, Key=key)
                    metadata = s3_response.get('Metadata', {})
                    
                    # Extraer datos de los metadatos
                    document_id = metadata.get('document-id')
                    client_id = metadata.get('client-id')
                    is_new_version = metadata.get('is-new-version') == 'true'

                    
                    logger.info(f"Metadatos: document_id={document_id}, client_id={client_id}, is_new_version={is_new_version}")
                    
                    # Si no tenemos un ID de documento en los metadatos, generar uno nuevo
                    if not document_id:
                        document_id = generate_uuid()
                        logger.info(f"ID de documento no encontrado en metadatos, generando nuevo: {document_id}")
                    
                    log_document_processing_end(
                        metadata_registro_id, 
                        estado='completado',
                        datos_procesados={"document_id": document_id, "client_id": client_id, "is_new_version": is_new_version}
                    )
                    
                except Exception as meta_error:
                    logger.error(f"Error al obtener metadatos de S3: {str(meta_error)}")
                    document_id = generate_uuid()
                    is_new_version = False
                    client_id = None
                    
                    log_document_processing_end(
                        metadata_registro_id, 
                        estado='error',
                        mensaje_error=str(meta_error),
                        datos_procesados={"document_id": document_id, "generated_new_id": True}
                    )
                
                # Actualizar el ID de registro ahora que tenemos un document_id
                record_registro_id = log_document_processing_start(
                    document_id,
                    'procesar_documento',
                    datos_entrada={"bucket": bucket, "key": key, "is_new_version": is_new_version},
                    analisis_id=lambda_registro_id
                )
                
                # Obtener metadatos completos del archivo
                file_metadata_registro_id = log_document_processing_start(
                    document_id,
                    'obtener_metadatos_archivo',
                    datos_entrada={"bucket": bucket, "key": key},
                    analisis_id=record_registro_id
                )
                
                try:
                    logger.info(f"Obteniendo metadatos del archivo: {bucket}/{key}")
                    file_metadata = get_file_metadata(bucket, key)
                    logger.info(f"Metadatos obtenidos: {json.dumps(file_metadata)}")
                    
                    log_document_processing_end(
                        file_metadata_registro_id, 
                        estado='completado',
                        datos_procesados={"filename": file_metadata.get('filename'), "content_type": file_metadata.get('content_type')}
                    )
                    
                except Exception as meta_error:
                    logger.error(f"Error al obtener metadatos: {str(meta_error)}")
                    # Crear metadatos básicos para continuar el flujo
                    filename = os.path.basename(key)
                    extension = filename.split('.')[-1] if '.' in filename else ''
                    file_metadata = {
                        'filename': filename,
                        'content_type': f"application/{extension}" if extension else "application/octet-stream",
                        'extension': extension,
                        'size': record['s3']['object'].get('size', 0),
                        'hash': 'unknown',
                        'last_modified': datetime.now().isoformat()
                    }
                    
                    log_document_processing_end(
                        file_metadata_registro_id, 
                        estado='completado_con_errores',
                        mensaje_error=str(meta_error),
                        datos_procesados={"filename": file_metadata.get('filename'), "content_type": file_metadata.get('content_type'), "generated": True}
                    )
                
                # Validar el documento
                validation_registro_id = log_document_processing_start(
                    document_id,
                    'validar_documento',
                    datos_entrada={"filename": file_metadata.get('filename'), "content_type": file_metadata.get('content_type')},
                    analisis_id=record_registro_id
                )
                
                try:
                    logger.info(f"Validando documento {document_id}")
                    validation_results = validate_document(file_metadata)
                    
                    if not validation_results['valid']:
                        logger.error(f"Documento inválido: {validation_results['errors']}")
                        
                        log_document_processing_end(
                            validation_registro_id, 
                            estado='error',
                            mensaje_error=str(validation_results['errors']),
                            datos_procesados={"valid": False}
                        )
                        
                        log_document_processing_end(
                            record_registro_id, 
                            estado='error',
                            mensaje_error=f"Documento inválido: {validation_results['errors']}"
                        )
                        
                        error_documents += 1
                        continue
                    
                    log_document_processing_end(
                        validation_registro_id, 
                        estado='completado',
                        datos_procesados={"valid": True}
                    )
                    
                except Exception as val_error:
                    logger.error(f"Error en validación: {str(val_error)}")
                    
                    log_document_processing_end(
                        validation_registro_id, 
                        estado='error',
                        mensaje_error=str(val_error),
                        datos_procesados={"valid": True, "assumed_valid": True}
                    )
                    # Continuar con el procesamiento asumiendo que es válido
                
                # Mover el documento a la carpeta de procesamiento
                move_registro_id = log_document_processing_start(
                    document_id,
                    'mover_documento',
                    datos_entrada={"source_bucket": bucket, "source_key": key, "dest_bucket": PROCESSED_BUCKET},
                    analisis_id=record_registro_id
                )
                
                filename = os.path.basename(key)
                dest_key = f"processing/{document_id}/{filename}"
                dest_bucket = PROCESSED_BUCKET
                success_copy = False
                
                try:
                    logger.info(f"Moviendo documento a {PROCESSED_BUCKET}/{dest_key}")
                    s3_operation_with_retry(
                        s3_client.copy_object,
                        CopySource={'Bucket': bucket, 'Key': key},
                        Bucket=PROCESSED_BUCKET,
                        Key=dest_key
                    )
                    logger.info(f"Archivo copiado correctamente")
                    success_copy = True
                    
                    copy_success_registro_id = log_document_processing_start(
                        document_id,
                        'eliminar_original',
                        datos_entrada={"original_bucket": bucket, "original_key": key},
                        analisis_id=move_registro_id
                    )
                    
                    # Eliminar el original solo si la copia fue exitosa
                    logger.info(f"Eliminando archivo original: {bucket}/{key}")
                    s3_operation_with_retry(
                        s3_client.delete_object,
                        Bucket=bucket,
                        Key=key
                    )
                    logger.info(f"Archivo original eliminado correctamente")
                    
                    log_document_processing_end(
                        copy_success_registro_id, 
                        estado='completado'
                    )
                    
                    log_document_processing_end(
                        move_registro_id, 
                        estado='completado',
                        datos_procesados={"dest_bucket": PROCESSED_BUCKET, "dest_key": dest_key}
                    )
                    
                except Exception as e:
                    logger.error(f"Error al copiar/eliminar el documento: {str(e)}")
                    # Si no podemos copiar, seguimos con la clave original
                    dest_bucket = bucket
                    dest_key = key
                    success_copy = False
                    
                    log_document_processing_end(
                        move_registro_id, 
                        estado='error',
                        mensaje_error=str(e),
                        datos_procesados={"using_original": True, "dest_bucket": dest_bucket, "dest_key": dest_key}
                    )
                
                # NUEVO: Verificar si es una nueva versión
                if is_new_version:
                    # Procesar como nueva versión
                    process_version_registro_id = log_document_processing_start(
                        document_id,
                        'procesar_como_nueva_version',
                        datos_entrada={"dest_bucket": dest_bucket, "dest_key": dest_key},
                        analisis_id=record_registro_id
                    )
                    
                    logger.info(f"Procesando documento {document_id} como una nueva versión")
                    version_result = process_new_document_version(document_id, file_metadata, dest_bucket, dest_key, client_id)
                    
                    log_document_processing_end(
                        process_version_registro_id, 
                        estado='completado',
                        datos_procesados={"result": version_result}
                    )
                else:
                    # Procesar como nuevo documento
                    process_new_registro_id = log_document_processing_start(
                        document_id,
                        'procesar_como_nuevo',
                        datos_entrada={"dest_bucket": dest_bucket, "dest_key": dest_key},
                        analisis_id=record_registro_id
                    )
                    
                    logger.info(f"Procesando documento {document_id} como nuevo")
                    new_result = process_new_document(document_id, file_metadata, dest_bucket, dest_key, client_id)
                    
                    log_document_processing_end(
                        process_new_registro_id, 
                        estado='completado',
                        datos_procesados={"result": new_result}
                    )
                
                log_document_processing_end(
                    record_registro_id, 
                    estado='completado'
                )
                
                processed_documents += 1
                
            except Exception as record_error:
                logger.error(f"Error al procesar registro: {str(record_error)}")
                logger.error(traceback.format_exc())
                
                if 'record_registro_id' in locals():
                    log_document_processing_end(
                        record_registro_id, 
                        estado='error',
                        mensaje_error=str(record_error)
                    )
                
                error_documents += 1
        
        summary = {
            'procesados': processed_documents,
            'errores': error_documents,
            'total': len(event.get('Records', []))
        }
        
        logger.info(f"Procesamiento completado: {processed_documents} exitosos, {error_documents} errores")
        
        log_document_processing_end(
            lambda_registro_id, 
            estado='completado',
            datos_procesados=summary
        )

        return {
            'statusCode': 200,
            'body': json.dumps('Procesamiento de documentos completado'),
            'summary': summary
        }
        
    except Exception as e:
        logger.error(f"Error general en lambda_handler: {str(e)}")
        logger.error(traceback.format_exc())
        
        log_document_processing_end(
            lambda_registro_id, 
            estado='error',
            mensaje_error=str(e)
        )
        
        return {
            'statusCode': 500,
            'body': json.dumps(f'Error: {str(e)}')
        }