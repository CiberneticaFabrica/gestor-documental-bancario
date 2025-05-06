# src/common/s3_utils.py
import os
import boto3
import logging
import hashlib
import mimetypes
from urllib.parse import unquote_plus

logger = logging.getLogger()
logger.setLevel(os.environ.get('LOG_LEVEL', 'INFO'))

s3_client = boto3.client('s3')

def get_s3_object(bucket, key):
    """Obtiene un objeto de S3"""
    try:
        response = s3_client.get_object(Bucket=bucket, Key=key)
        return response
    except Exception as e:
        logger.error(f"Error al obtener objeto S3 {bucket}/{key}: {str(e)}")
        raise

def download_s3_file(bucket, key, local_path):
    """Descarga un archivo de S3 a una ruta local"""
    try:
        # Asegurarnos de que existe el directorio
        os.makedirs(os.path.dirname(local_path), exist_ok=True)
        s3_client.download_file(bucket, key, local_path)
        logger.info(f"Archivo descargado: {bucket}/{key} -> {local_path}")
        return True
    except Exception as e:
        logger.error(f"Error al descargar archivo S3: {str(e)}")
        raise

def upload_file_to_s3(local_path, bucket, key):
    """Sube un archivo local a S3"""
    try:
        s3_client.upload_file(local_path, bucket, key)
        logger.info(f"Archivo subido: {local_path} -> {bucket}/{key}")
        return True
    except Exception as e:
        logger.error(f"Error al subir archivo a S3: {str(e)}")
        raise

def copy_s3_object(source_bucket, source_key, dest_bucket, dest_key):
    """Copia un objeto de S3 a otra ubicación"""
    try:
        copy_source = {'Bucket': source_bucket, 'Key': source_key}
        s3_client.copy_object(CopySource=copy_source, Bucket=dest_bucket, Key=dest_key)
        logger.info(f"Objeto copiado de {source_bucket}/{source_key} a {dest_bucket}/{dest_key}")
        return True
    except Exception as e:
        logger.error(f"Error al copiar objeto S3: {str(e)}")
        raise

def delete_s3_object(bucket, key):
    """Elimina un objeto de S3"""
    try:
        s3_client.delete_object(Bucket=bucket, Key=key)
        logger.info(f"Objeto eliminado: {bucket}/{key}")
        return True
    except Exception as e:
        logger.error(f"Error al eliminar objeto S3: {str(e)}")
        raise

def get_file_metadata(bucket, key):
    """Obtiene metadatos de un archivo en S3"""
    try:
        response = s3_client.head_object(Bucket=bucket, Key=key)
        
        # Extraer nombre y extensión
        filename = unquote_plus(key.split('/')[-1])
        extension = os.path.splitext(filename)[1].lower().lstrip('.')
        
        # Determinar mime type
        content_type = response.get('ContentType', 'application/octet-stream')
        if content_type == 'binary/octet-stream':
            content_type = mimetypes.guess_type(filename)[0] or 'application/octet-stream'
        
        # Usar ETag como hash en lugar de calcularlo
        etag = response.get('ETag', '').strip('"')
        
        return {
            'filename': filename,
            'content_type': content_type,
            'extension': extension,
            'size': response['ContentLength'],
            'hash': etag,
            'last_modified': response['LastModified'].isoformat(),
        }
    except Exception as e:
        logger.error(f"Error al obtener metadatos del archivo: {str(e)}")
        raise

def generate_s3_presigned_url(bucket, key, expiration=3600):
    """Genera una URL prefirmada para un objeto S3"""
    try:
        url = s3_client.generate_presigned_url(
            'get_object',
            Params={'Bucket': bucket, 'Key': key},
            ExpiresIn=expiration
        )
        return url
    except Exception as e:
        logger.error(f"Error al generar URL prefirmada: {str(e)}")
        raise