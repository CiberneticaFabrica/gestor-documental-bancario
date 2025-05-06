# src/upload_processor/document_validator.py
import os
import json
import logging
import re
import mimetypes
from datetime import datetime

logger = logging.getLogger()
logger.setLevel(os.environ.get('LOG_LEVEL', 'INFO'))

# Configuración de validación
MAX_FILE_SIZE = 20 * 1024 * 1024  # 20 MB
ALLOWED_MIME_TYPES = [
    'application/pdf',
    'image/jpeg',
    'image/png',
    'image/tiff',
    'application/msword',
    'application/vnd.openxmlformats-officedocument.wordprocessingml.document',
    'application/vnd.ms-excel',
    'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
]

def validate_document(metadata):
    """Valida un documento según reglas de negocio"""
    validation_results = {
        'valid': True,
        'errors': [],
        'warnings': []
    }
    
    # Validar tamaño
    if metadata['size'] > MAX_FILE_SIZE:
        validation_results['valid'] = False
        validation_results['errors'].append(
            f"El archivo excede el tamaño máximo permitido de {MAX_FILE_SIZE/1024/1024} MB"
        )
    
    # Validar tipo MIME
    if metadata['content_type'] not in ALLOWED_MIME_TYPES:
        validation_results['valid'] = False
        validation_results['errors'].append(
            f"El tipo de archivo {metadata['content_type']} no está permitido"
        )
    
    # Validaciones adicionales según extensión
    if metadata['extension'] == 'pdf':
        # Las validaciones PDF podrían incluir verificación de páginas, estructura, etc.
        if metadata['size'] < 10 * 1024:  # 10 KB
            validation_results['warnings'].append(
                "El PDF parece muy pequeño, podría estar corrupto o vacío"
            )
    elif metadata['extension'] in ['jpg', 'jpeg', 'png', 'tiff']:
        # Las validaciones de imagen podrían incluir verificación de resolución, etc.
        if metadata['size'] < 50 * 1024:  # 50 KB
            validation_results['warnings'].append(
                "La resolución de la imagen podría ser demasiado baja para procesamiento OCR efectivo"
            )
    
    return validation_results

def determine_document_type(metadata, filename):
    """
    Función de compatibilidad que llama a determine_document_type_from_metadata
    y devuelve solo el ID del tipo de documento.
    """
    result = determine_document_type_from_metadata(metadata, filename)
    return result.get('doc_type_id')

def determine_document_type_from_metadata(metadata, filename):
    """
    Determina el tipo de documento basado en metadatos y nombre de archivo.
    Retorna un diccionario con información de clasificación preliminar.
    """
    try:
        # Importar funciones de base de datos para obtener mappings
        from common.validation import get_document_types_map
        doc_type_map = get_document_types_map()
    except Exception as e:
        logger.error(f"Error al obtener tipos de documento: {str(e)}")
        # Mapa por defecto en caso de error
        doc_type_map = {
            'dni': '6b2b0c6b-26f4-11f0-8066-0affc7b8197b',
            'pasaporte': '6b2b1196-26f4-11f0-8066-0affc7b8197b',
            'contrato': '6b2b13cb-26f4-11f0-8066-0affc7b8197b',
            'nomina': '6b2b1516-26f4-11f0-8066-0affc7b8197b',
            'formulario_kyc': '6b2b163f-26f4-11f0-8066-0affc7b8197b',
            'extracto': '6b2b177c-26f4-11f0-8066-0affc7b8197b',
            'domicilio': '6b2b1888-26f4-11f0-8066-0affc7b8197b',
            'impuesto': '6b2b19ee-26f4-11f0-8066-0affc7b8197b'
        }
    
    # Normalizar nombre para análisis
    filename_lower = filename.lower()
    extension = metadata.get('extension', '').lower()
    
    # Mapeo de patrones en nombre a posibles tipos de documento
    patterns = {
        'dni': ['dni', 'identidad', 'id_', 'nif', 'documentoidentidad'],
        'pasaporte': ['pasap', 'passport', 'travel'],
        'contrato': ['contrato', 'contract', 'acuerdo', 'convenio', 'apertura'],
        'extracto': ['extracto', 'account', 'estado', 'cuenta', 'movimientos'],
        'nomina': ['nomina', 'payroll', 'salario', 'sueldo', 'retribución'],
        'impuesto': ['impuesto', 'tax', 'fiscal', 'irpf', 'renta', 'hacienda'],
        'domicilio': ['domicilio', 'residencia', 'dirección', 'vivienda'],
        'formulario_kyc': ['kyc', 'conoce', 'cliente', 'diligencia', 'compliance']
    }
    
    # Inicializamos la puntuación para cada tipo de documento
    scores = {doc_type: 0 for doc_type in patterns.keys()}
    
    # Evaluar coincidencias en el nombre
    for doc_type, keywords in patterns.items():
        for keyword in keywords:
            if keyword in filename_lower:
                scores[doc_type] += 3  # Mayor peso para coincidencias en nombre
                
    # Ajustar por extensión
    if extension == 'pdf':
        # Los PDF generalmente son contratos o extractos
        scores['contrato'] += 1
        scores['extracto'] += 1
    elif extension in ['jpg', 'jpeg', 'png']:
        # Las imágenes suelen ser documentos de identidad
        scores['dni'] += 1
        scores['pasaporte'] += 1
    elif extension in ['xls', 'xlsx']:
        # Excel suele ser nóminas o extractos
        scores['nomina'] += 2
        scores['extracto'] += 1
    
    # Determinar el tipo con mayor puntuación
    max_score = 0
    doc_type = "contrato"  # Tipo por defecto
    
    for dtype, score in scores.items():
        if score > max_score:
            max_score = score
            doc_type = dtype
            
    # Normalizar confianza entre 0.5 y 0.85
    # La confianza basada solo en metadatos nunca debe ser muy alta
    confidence = 0.5
    if max_score > 0:
        confidence = min(0.5 + (max_score * 0.05), 0.85)
    
    # Determinar si necesita Textract
    requires_textract = True
    
    # Los PDFs simples y documentos Office podrían procesarse con alternativas
    if extension == 'pdf' and metadata['size'] < 2 * 1024 * 1024:  # < 2MB
        requires_textract = confidence < 0.75  # Solo si no estamos seguros
    
    # Las imágenes siempre requieren Textract
    elif extension in ['jpg', 'jpeg', 'png', 'tiff']:
        requires_textract = True
    
    # Mapear a ID de base de datos
    doc_type_id = None
    if doc_type in doc_type_map:
        doc_type_id = doc_type_map[doc_type]
    else:
        # Buscar aproximado
        for key in doc_type_map:
            if doc_type in key or key in doc_type:
                doc_type_id = doc_type_map[key]
                break
                
    # Devolver un fallback si no se encuentra
    if not doc_type_id and doc_type_map:
        doc_type_id = list(doc_type_map.values())[0]
    
    return {
        'doc_type': doc_type,
        'doc_type_id': doc_type_id,
        'confidence': confidence,
        'requires_textract': requires_textract,
        'scores': scores
    }