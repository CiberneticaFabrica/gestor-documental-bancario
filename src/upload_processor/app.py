# src/upload_processor/app.py
import os
import json
import boto3
import logging
import uuid
import time
import sys
from datetime import datetime
from urllib.parse import unquote_plus

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
        link_document_to_client
    )
    from common.s3_utils import get_file_metadata, copy_s3_object, delete_s3_object
    from common.validation import validate_document, determine_document_type, get_document_expiry_info
    from common.pdf_utils import extract_text_with_pypdf2
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

def process_with_pypdf2(document_id, bucket, key, document_type_info, metadata):
    """
    Procesa un PDF con PyPDF2 en lugar de Textract para ahorrar costos
    """
    try:
        logger.info(f"Extrayendo texto con PyPDF2 para documento {document_id}")
        
        # Extraer texto con PyPDF2
        extracted_text = extract_text_with_pypdf2(bucket, key)
        
        if not extracted_text or extracted_text.startswith("ERROR:"):
            logger.error(f"Error al extraer texto con PyPDF2: {extracted_text}")
            return False
        
        # Determinar el tipo de documento con el texto extraído
        from common.validation import guess_document_type
        doc_type_info = guess_document_type(extracted_text)
        
        logger.info(f"Documento clasificado como {doc_type_info['document_type']} con confianza {doc_type_info['confidence']}")
        
        # Guardar texto extraído y resultados en base de datos
        update_analysis_record(
            document_id,
            extracted_text,
            None,  # entidades detectadas
            json.dumps(metadata),  # metadatos extracción
            'procesado_pypdf2',
            'pypdf2-1.0',
            0,  # tiempo procesamiento
            'upload_processor',
            doc_type_info['confidence'] < 0.85,  # requiere verificación
            False,  # verificado
            mensaje_error=None,
            confianza_clasificacion=doc_type_info['confidence'],
            tipo_documento=doc_type_info['document_type']
        )
        
        # Enviar mensaje a cola de clasificación indicando que ya hay texto extraído
        message = {
            'document_id': document_id,
            'bucket': bucket,
            'key': key,
            'skip_textract': True,
            'extraction_complete': True,
            'doc_type_info': doc_type_info,
            'content_type': metadata['content_type']
        }
        
        sqs_client.send_message(
            QueueUrl=CLASSIFICATION_QUEUE_URL,
            MessageBody=json.dumps(message)
        )
        
        logger.info(f"Documento {document_id} procesado con PyPDF2 y enviado a clasificación")
        return True
        
    except Exception as e:
        logger.error(f"Error en procesamiento PyPDF2: {str(e)}")
        return False

def start_textract_processing(document_id, bucket, key, document_type_info=None, preliminary_classification=None):
    """Inicia el procesamiento asíncrono con Textract"""
    try:
        # Verificar que tengamos las variables de entorno necesarias
        if not TEXTRACT_SNS_TOPIC or not TEXTRACT_ROLE_ARN:
            logger.warning("Variables de entorno para Textract asíncrono no configuradas")
            # Enviar directamente a clasificación sin procesamiento Textract
            message = {
                'document_id': document_id,
                'bucket': bucket,
                'key': key,
                'skip_textract': True,
                'document_type_info': document_type_info,
                'preliminary_classification': preliminary_classification
            }
            
            sqs_client.send_message(
                QueueUrl=CLASSIFICATION_QUEUE_URL,
                MessageBody=json.dumps(message)
            )
            
            logger.info(f"Documento {document_id} enviado a clasificación sin Textract (SNS no configurado)")
            return False
            
        # Optimización: Determinar el tipo de análisis según el tipo de documento
        feature_types = ['TABLES', 'FORMS']
        job_tag = f"{document_id}_textract"
        
        # Personalizar análisis según el tipo de documento preliminar
        if preliminary_classification and preliminary_classification.get('doc_type'):
            doc_type = preliminary_classification.get('doc_type').lower()
            
            # Para contratos usamos análisis más completo
            if doc_type == 'contrato':
                feature_types = ['TABLES', 'FORMS', 'QUERIES']
                job_tag = f"{document_id}_contract_analysis"
            
            # Para documentos de identidad priorizamos el texto
            elif doc_type in ['dni', 'pasaporte']:
                feature_types = ['FORMS']
                job_tag = f"{document_id}_id_analysis"
                
            # Para documentos financieros priorizamos tablas
            elif doc_type in ['extracto', 'nomina', 'impuesto']:
                feature_types = ['TABLES', 'FORMS']
                job_tag = f"{document_id}_financial_analysis"
        
        # Configurar notificación SNS para cuando termine el proceso
        logger.info(f"Iniciando procesamiento asíncrono con Textract para {document_id}")
        
        # Preparamos parámetros de Textract optimizados
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
        
        # Añadir QueriesConfig si es un contrato u otro documento complejo
        if 'QUERIES' in feature_types:
            # Especificar consultas que extraerán información clave en una sola llamada
            textract_params['QueriesConfig'] = {
                'Queries': [
                    {'Text': 'What is the document type?'},
                    {'Text': 'What is the document number?'},
                    {'Text': 'What is the issue date?'},
                    {'Text': 'What is the expiration date?'},
                    {'Text': 'Who is the person named in this document?'},
                    {'Text': 'What is the main purpose of this document?'}
                ]
            }
        
        # Iniciar análisis de documento (proceso asíncrono)
        textract_response = textract_client.start_document_analysis(**textract_params)
        
        job_id = textract_response['JobId']
        logger.info(f"Iniciado procesamiento asíncrono de Textract: JobId={job_id}")
        
        # Actualizar en la base de datos con todos los detalles
        try:
            update_document_processing_status(
                document_id, 
                'textract_iniciado',
                json.dumps({
                    'textract_job_id': job_id,
                    'textract_job_type': 'ANALYSIS',
                    'feature_types': feature_types,
                    'preliminary_classification': preliminary_classification
                })
            )
        except Exception as db_error:
            logger.error(f"Error al actualizar estado en BD: {str(db_error)}, pero continuando procesamiento")
        
        return True
        
    except Exception as e:
        logger.error(f"Error al iniciar procesamiento Textract: {str(e)}")
        # En caso de error, enviar directamente a la cola de clasificación
        try:
            message = {
                'document_id': document_id,
                'bucket': bucket,
                'key': key,
                'error': True,
                'error_message': str(e),
                'skip_textract': True,
                'document_type_info': document_type_info,
                'preliminary_classification': preliminary_classification
            }
            
            sqs_client.send_message(
                QueueUrl=CLASSIFICATION_QUEUE_URL,
                MessageBody=json.dumps(message)
            )
            
            logger.info(f"Documento {document_id} enviado a clasificación tras error en Textract")
        except Exception as sqs_error:
            logger.error(f"Error al enviar mensaje a SQS: {str(sqs_error)}")
        
        return False

def process_document_metadata(document_id, metadata, bucket, key):
    """Procesa los metadatos del documento y actualiza la base de datos"""
    try:
        # Generar IDs únicos para versión y análisis
        version_id = generate_uuid()
        analysis_id = generate_uuid()
        
        # Clasificación preliminar basada en metadatos y nombre
        preliminary_classification = determine_document_type_from_metadata(metadata, metadata['filename'])
        
        # Obtener ID de tipo de documento
        doc_type_id = preliminary_classification.get('doc_type_id')
        
        # Si no tenemos un ID de tipo documento, usamos la función anterior por compatibilidad
        if not doc_type_id:
            doc_type_id = determine_document_type(metadata, metadata['filename'])
        
        # Obtener información del tipo de documento
        doc_type_info = get_document_type_by_id(doc_type_id)
        
        # Generar código de documento con prefijo apropiado
        prefijo = doc_type_info.get('prefijo_nomenclatura', 'DOC') if doc_type_info else 'DOC'
        timestamp = int(time.time())
        doc_code = f"{prefijo}-{timestamp}-{document_id[:8]}"
        
        # Obtener información de categoría bancaria
        cat_info = get_banking_doc_category(doc_type_id)
        requiere_validacion = cat_info.get('requiere_validacion', True) if cat_info else True
        validez_dias = cat_info.get('validez_en_dias') if cat_info else None
        

        # Obtener ID del cliente desde metadatos de S3 si existe
       
        client_id = None
        try:
            s3_response = s3_client.head_object(Bucket=bucket, Key=key)
            client_id = s3_response.get('Metadata', {}).get('client-id')
        except Exception as e:
            logger.warning(f"No se pudieron obtener metadatos adicionales: {str(e)}")
        
        logger.info(f"Vincular documento al cliente")
        # Si tenemos ID de cliente, vincular el documento
        if client_id:
            try:
                link_result = link_document_to_client(document_id, client_id)
                logger.info(f"Documento {document_id} vinculado a cliente {client_id}: {link_result}")
            except Exception as link_error:
                logger.error(f"Error al vincular documento con cliente: {str(link_error)}")
    
 
        # Preparar datos para inserción en base de datos
        document_data = {
            'id_documento': document_id,
            'codigo_documento': doc_code,
            'id_tipo_documento': doc_type_id,
            'titulo': metadata['filename'],
            'descripcion': f"Documento cargado automáticamente para procesamiento",
            'version_actual': 1,
            'fecha_creacion': datetime.now().isoformat(),
            'fecha_modificacion': datetime.now().isoformat(),
            'creado_por': 'admin-uuid-0001',
            'modificado_por': 'admin-uuid-0001',
            'id_carpeta': None,
            'estado': 'PENDIENTE_PROCESAMIENTO',
            'confianza_extraccion': preliminary_classification.get('confidence', 0.5),
            'validado_manualmente': False,
            'metadatos': json.dumps({
                'formato': metadata['extension'],
                'tamano': metadata['size'],
                'origen': 'carga_automatica',
                'requiere_validacion': requiere_validacion,
                'validez_dias': validez_dias,
                'categoria_bancaria': cat_info.get('nombre_categoria') if cat_info else None,
                'preliminary_classification': preliminary_classification
            })
        }
        
        version_data = {
            'id_version': version_id,
            'id_documento': document_id,
            'numero_version': 1,
            'fecha_creacion': datetime.now().isoformat(),
            'creado_por': 'admin-uuid-0001',
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
        
        analysis_data = {
            'id_analisis': analysis_id,
            'id_documento': document_id,
            'tipo_documento': doc_type_info.get('nombre_tipo', 'documento') if doc_type_info else 'documento',
            'estado_analisis': 'iniciado',
            'fecha_analisis': datetime.now().isoformat(),
            'confianza_clasificacion': preliminary_classification.get('confidence', 0.5),
            'texto_extraido': None,
            'entidades_detectadas': None,
            'metadatos_extraccion': json.dumps(preliminary_classification),
            'mensaje_error': None,
            'version_modelo': 'clasificacion-inicial',
            'tiempo_procesamiento': 0,
            'procesado_por': 'upload_processor',
            'requiere_verificacion': requiere_validacion,
            'verificado': False,
            'verificado_por': None,
            'fecha_verificacion': None
        }
        
        # Insertar en base de datos con manejo de errores
        try:
            logger.info(f"Insertando documento {document_id} en base de datos")
            insert_document(document_data)
            insert_document_version(version_data)
            insert_analysis_record(analysis_data)
            logger.info(f"Documento {document_id} registrado en base de datos correctamente")
            
            # Devolver información del tipo de documento para usar en el procesamiento
            return {
                'analysis_id': analysis_id,
                'document_type_id': doc_type_id,
                'document_type_info': doc_type_info,
                'category_info': cat_info,
                'requires_validation': requiere_validacion,
                'expiry_days': validez_dias,
                'preliminary_classification': preliminary_classification
            }
            
        except Exception as db_error:
            logger.error(f"Error al insertar en base de datos: {str(db_error)}")
            # Continuar proceso incluso con error de BD
            return {
                'analysis_id': analysis_id,
                'document_type_id': doc_type_id,
                'document_type_info': doc_type_info,
                'error': str(db_error),
                'preliminary_classification': preliminary_classification
            }
    except Exception as e:
        logger.error(f"Error en process_document_metadata: {str(e)}")
        return {'analysis_id': generate_uuid(), 'error': str(e)}

def lambda_handler(event, context):
    """
    Función principal que procesa la carga de documentos.
    Se activa cuando se carga un documento en el bucket de S3.
    """
    try:
        logger.info(f"Evento recibido: {json.dumps(event)}")
        
        # Procesar cada registro del evento S3
        for record in event['Records']:
            # Obtener información del bucket y clave
            bucket = record['s3']['bucket']['name']
            
            # Decodificar la clave URL para manejar caracteres especiales
            key = unquote_plus(record['s3']['object']['key'])
            
            logger.info(f"Procesando documento: {bucket}/{key}")
            
            # Generar ID único para el documento
            document_id = generate_uuid()
            logger.info(f"ID de documento asignado: {document_id}")
            
            # Verificar que el objeto existe antes de continuar
            if not check_s3_object_exists(bucket, key):
                logger.error(f"El objeto {bucket}/{key} no existe, saltando procesamiento")
                continue
            
            # Obtener metadatos completos del archivo
            try:
                logger.info(f"Obteniendo metadatos del archivo: {bucket}/{key}")
                metadata = get_file_metadata(bucket, key)
                logger.info(f"Metadatos obtenidos: {json.dumps(metadata)}")
            except Exception as meta_error:
                logger.error(f"Error al obtener metadatos: {str(meta_error)}")
                # Crear metadatos básicos para continuar el flujo
                filename = os.path.basename(key)
                extension = filename.split('.')[-1] if '.' in filename else ''
                metadata = {
                    'filename': filename,
                    'content_type': f"application/{extension}" if extension else "application/octet-stream",
                    'extension': extension,
                    'size': record['s3']['object'].get('size', 0),
                    'hash': 'unknown',
                    'last_modified': datetime.now().isoformat()
                }
            
            # Validar el documento
            try:
                logger.info(f"Validando documento {document_id}")
                validation_results = validate_document(metadata)
                if not validation_results['valid']:
                    logger.error(f"Documento inválido: {validation_results['errors']}")
                    # Mover el documento a una carpeta de "inválidos"
                    invalid_key = f"invalid/{int(time.time())}-{os.path.basename(key)}"
                    logger.info(f"Moviendo documento inválido a {PROCESSED_BUCKET}/{invalid_key}")
                    try:
                        copy_s3_object(bucket, key, PROCESSED_BUCKET, invalid_key)
                        delete_s3_object(bucket, key)
                        logger.info(f"Documento inválido movido correctamente")
                        
                        # Registrar en auditoría
                        try:
                            audit_data = {
                                'fecha_hora': datetime.now().isoformat(),
                                'usuario_id': 'admin-uuid-0001',
                                'direccion_ip': obtener_ip_origen(context),
                                'accion': 'rechazar',
                                'entidad_afectada': 'documento',
                                'id_entidad_afectada': document_id,
                                'detalles': json.dumps({
                                    'bucket': bucket,
                                    'key': key,
                                    'errors': validation_results['errors'],
                                    'filename': metadata['filename']
                                }),
                                'resultado': 'error'
                            }
                            insert_audit_record(audit_data)
                        except Exception as audit_error:
                            logger.error(f"Error al registrar auditoría: {str(audit_error)}")
                            
                    except Exception as move_error:
                        logger.error(f"Error al mover documento inválido: {str(move_error)}")
                    continue
            except Exception as val_error:
                logger.error(f"Error en validación: {str(val_error)}")
                # Continuar con el procesamiento asumiendo que es válido
            
            # Mover el documento a la carpeta de procesamiento
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
                
                # Eliminar el original solo si la copia fue exitosa
                logger.info(f"Eliminando archivo original: {bucket}/{key}")
                
                s3_operation_with_retry(
                    s3_client.delete_object,
                    Bucket=bucket,
                    Key=key
                )
                
                logger.info(f"Archivo original eliminado correctamente")
            except Exception as e:
                logger.error(f"Error al copiar/eliminar el documento: {str(e)}")
                # Si no podemos copiar, seguimos con la clave original
                dest_bucket = bucket
                dest_key = key
                success_copy = False
            
            # CAMBIO IMPORTANTE: Realizar clasificación preliminar basada en metadatos
            preliminary_classification = determine_document_type_from_metadata(metadata, metadata['filename'])
            logger.info(f"Clasificación preliminar: {json.dumps(preliminary_classification)}")
            
            # Procesar metadatos y registrar en la base de datos
            try:
                logger.info(f"Procesando metadatos del documento {document_id}")
                doc_metadata = process_document_metadata(document_id, metadata, dest_bucket, dest_key)
                logger.info(f"Metadatos procesados: {json.dumps(doc_metadata)}")
                
                # Registrar en auditoría
                try:
                    audit_data = {
                        'fecha_hora': datetime.now().isoformat(),
                        'usuario_id': 'admin-uuid-0001',
                        'direccion_ip': obtener_ip_origen(context),
                        'accion': 'crear',
                        'entidad_afectada': 'documento',
                        'id_entidad_afectada': document_id,
                        'detalles': json.dumps({
                            'bucket': dest_bucket,
                            'key': dest_key,
                            'content_type': metadata['content_type'],
                            'size': metadata['size'],
                            'document_type': doc_metadata.get('document_type_info', {}).get('nombre_tipo', 'desconocido'),
                            'preliminary_classification': preliminary_classification
                        }),
                        'resultado': 'exito'
                    }
                    insert_audit_record(audit_data)
                except Exception as audit_error:
                    logger.error(f"Error al registrar auditoría: {str(audit_error)}")
                
                # DECISIÓN IMPORTANTE: Determinar si usamos Textract o procesamiento local
                if success_copy:  # Solo si la copia fue exitosa
                    needs_textract = should_use_textract(metadata, 
                                                       doc_metadata.get('document_type_info'), 
                                                       preliminary_classification)
                    
                    if needs_textract:
                        # Usar Textract para procesamiento completo
                        logger.info(f"Iniciando procesamiento Textract para documento {document_id}")
                        start_textract_processing(
                            document_id, 
                            dest_bucket, 
                            dest_key, 
                            doc_metadata.get('document_type_info'),
                            preliminary_classification
                        )
                    else:
                        # Intentar con procesamiento local (PyPDF2)
                        logger.info(f"Intentando procesamiento local para documento {document_id}")
                        success = process_with_pypdf2(
                            document_id, 
                            dest_bucket, 
                            dest_key, 
                            doc_metadata.get('document_type_info'),
                            metadata
                        )
                        
                        if not success:
                            # Si falla PyPDF2, caer en Textract
                            logger.info(f"Procesamiento local falló, iniciando Textract para {document_id}")
                            start_textract_processing(
                                document_id, 
                                dest_bucket, 
                                dest_key, 
                                doc_metadata.get('document_type_info'),
                                preliminary_classification
                            )
                else:
                    # Si no se pudo copiar el archivo, enviar directamente a clasificación
                    message = {
                        'document_id': document_id,
                        'bucket': dest_bucket,
                        'key': dest_key,
                        'skip_textract': True,
                        'document_type_info': doc_metadata.get('document_type_info'),
                        'category_info': doc_metadata.get('category_info'),
                        'preliminary_classification': preliminary_classification
                    }
                    
                    sqs_client.send_message(
                        QueueUrl=CLASSIFICATION_QUEUE_URL,
                        MessageBody=json.dumps(message)
                    )
                    
                    logger.info(f"Documento {document_id} enviado a clasificación (copia fallida)")
                
            except Exception as process_error:
                logger.error(f"Error al procesar documento: {str(process_error)}")
        
        return {
            'statusCode': 200,
            'body': json.dumps('Procesamiento de documentos completado')
        }
        
    except Exception as e:
        logger.error(f"Error general en lambda_handler: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps(f'Error: {str(e)}')
        }