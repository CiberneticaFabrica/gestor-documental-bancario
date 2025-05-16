# src/thumbnail_generator/app.py
import os
import json
import boto3
import logging
import uuid
from PIL import Image
import io
import fitz  # PyMuPDF para PDFs
from datetime import datetime

# Configurar el logger
logger = logging.getLogger()
logger.setLevel(os.environ.get('LOG_LEVEL', 'INFO'))

# Inicializar clientes AWS
s3_client = boto3.client('s3')
lambda_client = boto3.client('lambda')

# Obtener variables de entorno
THUMBNAILS_BUCKET = os.environ.get('THUMBNAILS_BUCKET', os.environ.get('PROCESSED_BUCKET'))

# Importar funciones de utilidad
from common.db_connector import execute_query

def update_version_thumbnails(version_id, thumbnail_location):
    """Actualiza la versión del documento para indicar que las miniaturas están generadas"""
    logger.info(f"Intentando actualizar versión {version_id} con ubicación de miniatura: {thumbnail_location}")
    
    # Primero verificar si la versión existe
    check_query = """
    SELECT id_version, miniaturas_generadas, ubicacion_miniatura, ubicacion_almacenamiento_ruta 
    FROM versiones_documento
    WHERE id_version = %s
    """
    
    try:
        check_result = execute_query(check_query, (version_id,))
        if not check_result:
            logger.error(f"Error: No se encontró la versión {version_id} en la base de datos")
            return False
            
        logger.info(f"Versión existente encontrada: {check_result[0]}")
        
        # Proceder con la actualización
        query = """
        UPDATE versiones_documento
        SET miniaturas_generadas = 1,
            ubicacion_miniatura = %s
        WHERE id_version = %s
        """
        
        result = execute_query(query, (thumbnail_location, version_id), fetch=False)
        logger.info(f"Update ejecutado. Resultado: {result}")
        
        # Verificar que la actualización fue exitosa
        verify_query = """
        SELECT id_version, miniaturas_generadas, ubicacion_miniatura 
        FROM versiones_documento
        WHERE id_version = %s
        """
        
        verify_result = execute_query(verify_query, (version_id,))
        if verify_result:
            logger.info(f"Verificación post-actualización: {verify_result[0]}")
            if verify_result[0]['miniaturas_generadas'] == 1 and verify_result[0]['ubicacion_miniatura'] == thumbnail_location:
                logger.info(f"✅ Actualización confirmada para versión {version_id}")
            else:
                logger.warning(f"⚠️ La actualización parece no haberse aplicado correctamente: {verify_result[0]}")
        
        return True
    except Exception as e:
        logger.error(f"Error al actualizar versión con miniatura: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        return False

def generate_pdf_thumbnail(bucket, key, output_bucket, output_key):
    """Genera una miniatura para un PDF"""
    logger.info(f"Iniciando generación de miniatura PDF. Bucket: {bucket}, Key: {key}")
    logger.info(f"Destino: Bucket: {output_bucket}, Key: {output_key}")
    
    try:
        # Verificar que los buckets existen antes de intentar acceder
        try:
            s3_client.head_bucket(Bucket=bucket)
            logger.info(f"✅ Bucket origen {bucket} existe y es accesible")
        except Exception as bucket_error:
            logger.error(f"❌ Error al acceder al bucket origen {bucket}: {str(bucket_error)}")
            return False
            
        try:
            s3_client.head_bucket(Bucket=output_bucket)
            logger.info(f"✅ Bucket destino {output_bucket} existe y es accesible")
        except Exception as bucket_error:
            logger.error(f"❌ Error al acceder al bucket destino {output_bucket}: {str(bucket_error)}")
            return False
        
        # Intentar listar contenido del bucket para verificar permisos
        try:
            s3_client.list_objects_v2(Bucket=bucket, Prefix=key, MaxKeys=1)
            logger.info(f"✅ Tenemos permisos de listado en {bucket}")
        except Exception as list_error:
            logger.error(f"❌ Error al listar objetos en bucket {bucket}: {str(list_error)}")
        
        # Descargar PDF de S3
        logger.info(f"Descargando PDF desde {bucket}/{key}")
        try:
            response = s3_client.get_object(Bucket=bucket, Key=key)
            pdf_data = response['Body'].read()
            logger.info(f"✅ PDF descargado correctamente. Tamaño: {len(pdf_data)} bytes")
        except Exception as download_error:
            logger.error(f"❌ Error al descargar PDF: {str(download_error)}")
            return False
        
        # Abrir PDF con PyMuPDF
        logger.info(f"Abriendo PDF con PyMuPDF")
        try:
            pdf = fitz.open(stream=pdf_data, filetype="pdf")
            logger.info(f"✅ PDF abierto correctamente. Páginas: {pdf.page_count}")
            
            # Verificar que el PDF tenga al menos una página
            if pdf.page_count == 0:
                logger.error(f"El PDF no tiene páginas: {bucket}/{key}")
                return False
            
            # Obtener primera página
            page = pdf[0]
            logger.info(f"✅ Primera página obtenida. Tamaño: {page.rect}")
        except Exception as pdf_error:
            logger.error(f"❌ Error al procesar el PDF con PyMuPDF: {str(pdf_error)}")
            return False
        
        # Renderizar la página como imagen (resolución 150 DPI)
        logger.info(f"Renderizando página como imagen")
        try:
            pix = page.get_pixmap(matrix=fitz.Matrix(1.5, 1.5))
            logger.info(f"✅ Página renderizada. Tamaño: {pix.width}x{pix.height} píxeles")
        except Exception as render_error:
            logger.error(f"❌ Error al renderizar página: {str(render_error)}")
            return False
        
        # Convertir a imagen PIL
        logger.info(f"Convirtiendo a imagen PIL")
        try:
            img = Image.frombytes("RGB", [pix.width, pix.height], pix.samples)
            logger.info(f"✅ Imagen PIL creada. Tamaño: {img.size}, Modo: {img.mode}")
        except Exception as pil_error:
            logger.error(f"❌ Error al convertir a imagen PIL: {str(pil_error)}")
            return False
        
        # Redimensionar la imagen
        logger.info(f"Redimensionando imagen")
        try:
            width, height = img.size
            thumbnail_width = 300
            thumbnail_height = int(height * (thumbnail_width / width))
            img = img.resize((thumbnail_width, thumbnail_height), Image.LANCZOS)
            logger.info(f"✅ Imagen redimensionada. Nuevo tamaño: {img.size}")
        except Exception as resize_error:
            logger.error(f"❌ Error al redimensionar imagen: {str(resize_error)}")
            return False
        
        # Guardar imagen en buffer
        logger.info(f"Guardando imagen en buffer")
        try:
            buffer = io.BytesIO()
            img.save(buffer, format="JPEG", quality=85)
            buffer.seek(0)
            buffer_size = buffer.getbuffer().nbytes
            logger.info(f"✅ Imagen guardada en buffer. Tamaño: {buffer_size} bytes")
        except Exception as buffer_error:
            logger.error(f"❌ Error al guardar en buffer: {str(buffer_error)}")
            return False
        
        # Subir imagen a S3
        logger.info(f"Subiendo imagen a S3: {output_bucket}/{output_key}")
        try:
            s3_client.put_object(
                Bucket=output_bucket,
                Key=output_key,
                Body=buffer,
                ContentType='image/jpeg'
            )
            logger.info(f"✅ Imagen subida correctamente a S3")
            
            # Verificar que la imagen fue subida correctamente
            try:
                head = s3_client.head_object(Bucket=output_bucket, Key=output_key)
                logger.info(f"✅ Verificación de subida exitosa. Tamaño: {head.get('ContentLength')} bytes")
            except Exception as verify_error:
                logger.error(f"❌ Error al verificar la subida: {str(verify_error)}")
                return False
                
        except Exception as upload_error:
            logger.error(f"❌ Error al subir imagen a S3: {str(upload_error)}")
            return False
        
        logger.info(f"✅ TODO CORRECTO: Miniatura de PDF generada y guardada en {output_bucket}/{output_key}")
        return True
    except Exception as e:
        logger.error(f"❌ Error general al generar miniatura de PDF: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        return False

def generate_image_thumbnail(bucket, key, output_bucket, output_key):
    """Genera una miniatura para una imagen"""
    try:
        # Descargar imagen de S3
        response = s3_client.get_object(Bucket=bucket, Key=key)
        image_data = response['Body'].read()
        
        # Abrir imagen con PIL
        with Image.open(io.BytesIO(image_data)) as img:
            # Redimensionar la imagen a un tamaño razonable para una miniatura (300px de ancho)
            width, height = img.size
            thumbnail_width = 300
            thumbnail_height = int(height * (thumbnail_width / width))
            img = img.resize((thumbnail_width, thumbnail_height), Image.LANCZOS)
            
            # Guardar imagen en buffer
            buffer = io.BytesIO()
            img.save(buffer, format="JPEG", quality=85)
            buffer.seek(0)
            
            # Subir imagen a S3
            s3_client.put_object(
                Bucket=output_bucket,
                Key=output_key,
                Body=buffer,
                ContentType='image/jpeg'
            )
            
        logger.info(f"Miniatura de imagen generada y guardada en {output_bucket}/{output_key}")
        return True
    except Exception as e:
        logger.error(f"Error generando miniatura de imagen: {str(e)}")
        return False

def check_environment():
    """Verifica la configuración y permisos necesarios"""
    logger.info("=== VERIFICANDO ENTORNO Y PERMISOS ===")
    
    # Verificar variables de entorno
    required_vars = ['THUMBNAILS_BUCKET', 'PROCESSED_BUCKET', 'DB_HOST', 'DB_NAME', 'DB_USER', 'DB_PASSWORD']
    for var in required_vars:
        value = os.environ.get(var)
        if value:
            # Ocultar credenciales en logs
            if 'PASSWORD' in var:
                logger.info(f"✅ Variable {var} configurada (valor oculto)")
            else:
                logger.info(f"✅ Variable {var} configurada: {value}")
        else:
            logger.warning(f"⚠️ Variable {var} no configurada")
    
    # Verificar conexión a base de datos
    try:
        logger.info("Verificando conexión a base de datos...")
        conn = get_connection()
        logger.info("✅ Conexión a base de datos exitosa")
        conn.close()
    except Exception as db_error:
        logger.error(f"❌ Error al conectar a la base de datos: {str(db_error)}")
    
    # Verificar acceso a S3
    try:
        thumbnails_bucket = os.environ.get('THUMBNAILS_BUCKET', os.environ.get('PROCESSED_BUCKET'))
        logger.info(f"Verificando acceso a bucket S3: {thumbnails_bucket}")
        s3_client.head_bucket(Bucket=thumbnails_bucket)
        logger.info(f"✅ Acceso a bucket S3 {thumbnails_bucket} verificado")
        
        # Intentar listar objetos
        response = s3_client.list_objects_v2(Bucket=thumbnails_bucket, MaxKeys=1)
        logger.info(f"✅ Permiso para listar objetos verificado. Respuesta: {response}")
        
        # Intentar put y delete para verificar permisos de escritura
        test_key = f"test/lambda_permissions_check_{uuid.uuid4()}.txt"
        s3_client.put_object(Bucket=thumbnails_bucket, Key=test_key, Body=b"Test permissions")
        logger.info(f"✅ Permiso de escritura verificado")
        s3_client.delete_object(Bucket=thumbnails_bucket, Key=test_key)
        logger.info(f"✅ Permiso de eliminación verificado")
    except Exception as s3_error:
        logger.error(f"❌ Error al verificar acceso a S3: {str(s3_error)}")
    
    logger.info("=== FIN DE VERIFICACIÓN DE ENTORNO ===")

def lambda_handler(event, context):
    """Función principal que procesa mensajes de la cola SQS"""
    try:
        logger.info(f"Evento recibido: {json.dumps(event)}")
        logger.info(f"Variables de entorno: THUMBNAILS_BUCKET={THUMBNAILS_BUCKET}, PROCESSED_BUCKET={os.environ.get('PROCESSED_BUCKET')}")
        # Verificar configuración y permisos
        check_environment()
        # Procesar mensajes de SQS
        for record in event.get('Records', []):
            # Obtener mensaje
            try:
                message = json.loads(record['body'])
                logger.info(f"Mensaje decodificado: {json.dumps(message)}")
            except json.JSONDecodeError:
                logger.error(f"Error decodificando mensaje: {record['body']}")
                continue
            
            # Extraer información del mensaje
            document_id = message.get('document_id')
            version_id = message.get('version_id')
            bucket = message.get('bucket')
            key = message.get('key')
            extension = message.get('extension', '').lower()
            mime_type = message.get('mime_type', '')
            
            logger.info(f"Procesando documento: {document_id}, versión: {version_id}")
            logger.info(f"Detalles: bucket={bucket}, key={key}, extension={extension}, mime_type={mime_type}")
            
            # Verificar que tenemos toda la información necesaria
            if not all([document_id, version_id, bucket, key]):
                logger.error(f"Faltan datos requeridos en mensaje: {message}")
                continue
            
            # Crear nombre base para la miniatura
            file_name = os.path.basename(key)
            file_base = os.path.splitext(file_name)[0]
            thumbnail_key = f"{os.path.dirname(key)}/{file_base}_thumbnail.jpg"
            
            logger.info(f"Nombre de archivo original: {file_name}")
            logger.info(f"Nombre base para miniatura: {file_base}")
            logger.info(f"Ruta de miniatura generada: {thumbnail_key}")
            
            # Configurar la ruta completa de la miniatura para la base de datos
            thumbnail_location = f"{THUMBNAILS_BUCKET}/{thumbnail_key}"
            logger.info(f"Ruta completa para BD: {thumbnail_location}")
            
            success = False
            
            # Generar miniatura según el tipo de documento
            if extension == 'pdf' or mime_type == 'application/pdf':
                logger.info(f"Generando miniatura para PDF")
                success = generate_pdf_thumbnail(bucket, key, THUMBNAILS_BUCKET, thumbnail_key)
            elif extension in ['jpg', 'jpeg', 'png', 'tiff'] or mime_type.startswith('image/'):
                logger.info(f"Generando miniatura para imagen")
                success = generate_image_thumbnail(bucket, key, THUMBNAILS_BUCKET, thumbnail_key)
            else:
                logger.warning(f"Tipo de documento no soportado para miniaturas: {extension}")
                # Para documentos no soportados, crear una imagen genérica
            
            # Actualizar la base de datos si la generación fue exitosa
            if success:
                logger.info(f"Generación exitosa. Actualizando base de datos...")
                update_result = update_version_thumbnails(version_id, thumbnail_location)
                if update_result:
                    logger.info(f"✅ Proceso completo exitoso para documento {document_id}")
                else:
                    logger.error(f"❌ Fallo al actualizar base de datos para documento {document_id}")
            else:
                logger.error(f"❌ No se pudo generar miniatura para documento {document_id}")
                
        return {
            'statusCode': 200,
            'body': json.dumps('Procesamiento de miniaturas completado')
        }
    except Exception as e:
        logger.error(f"❌ Error general en lambda_handler: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        return {
            'statusCode': 500,
            'body': json.dumps(f'Error: {str(e)}')
        }
    