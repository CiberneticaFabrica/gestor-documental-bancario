# src/common/validation.py
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

# Patrones para documentos bancarios
DNI_PATTERN = r"(?i)\b\d{8}[A-Za-z]?\b"
PASSPORT_PATTERN = r"(?i)\b[A-Z]{1,2}[0-9]{6,7}\b"
IBAN_PATTERN = r"(?i)\b[A-Z]{2}[0-9]{2}[A-Z0-9]{4}[0-9]{7}([A-Z0-9]?){0,16}\b"
CREDIT_CARD_PATTERN = r"(?i)\b(?:\d[ -]*?){13,16}\b"

# Caché para almacenar datos obtenidos de la base de datos
# Esto evita consultas repetidas a la base de datos
_doc_types_cache = None
_doc_categories_cache = None
_validation_requirements_cache = None
_cache_timestamp = None
_CACHE_TIMEOUT = 3600  # 1 hora en segundos

def _is_cache_valid():
    """Verifica si la caché es válida o ha expirado"""
    if _cache_timestamp is None:
        return False
    elapsed = (datetime.now() - _cache_timestamp).total_seconds()
    return elapsed < _CACHE_TIMEOUT

def get_document_types_map():
    """Obtiene el mapa de tipos de documento desde la base de datos"""
    global _doc_types_cache, _cache_timestamp
    
    # Si la caché es válida, devolver los datos en caché
    if _doc_types_cache is not None and _is_cache_valid():
        return _doc_types_cache
    
    try:
        # Importar aquí para evitar dependencias circulares
        from common.db_connector import execute_query
        
        query = """
        SELECT id_tipo_documento, nombre_tipo, prefijo_nomenclatura 
        FROM tipos_documento 
        WHERE es_documento_bancario = 1
        """
        
        results = execute_query(query)
        doc_type_map = {}
        
        # Crear mapa de nombres normalizados a IDs
        for doc_type in results:
            # Normalizar nombre (quitar espacios, minúsculas)
            normalized_name = doc_type['nombre_tipo'].lower().replace(' ', '_')
            doc_type_map[normalized_name] = doc_type['id_tipo_documento']
            
            # También mapear por prefijo para facilitar búsqueda
            if doc_type['prefijo_nomenclatura']:
                prefix_key = doc_type['prefijo_nomenclatura'].lower()
                doc_type_map[prefix_key] = doc_type['id_tipo_documento']
        
        # Actualizar caché
        _doc_types_cache = doc_type_map
        _cache_timestamp = datetime.now()
        
        return doc_type_map
    except Exception as e:
        logger.error(f"Error al obtener tipos de documento: {str(e)}")
        # Si hay un error, devolver un mapa por defecto
        default_map = {
            'dni': '6b2b0c6b-26f4-11f0-8066-0affc7b8197b',
            'pasaporte': '6b2b1196-26f4-11f0-8066-0affc7b8197b',
            'contrato_cuenta': '6b2b13cb-26f4-11f0-8066-0affc7b8197b',
            'nomina': '6b2b1516-26f4-11f0-8066-0affc7b8197b',
            'formulario_kyc': '6b2b163f-26f4-11f0-8066-0affc7b8197b',
            'extracto_bancario': '6b2b177c-26f4-11f0-8066-0affc7b8197b',
            'comprobante_domicilio': '6b2b1888-26f4-11f0-8066-0affc7b8197b',
            'declaracion_impuestos': '6b2b19ee-26f4-11f0-8066-0affc7b8197b'
        }
        return default_map

def get_document_categories_map():
    """Obtiene el mapa de categorías bancarias desde la base de datos"""
    global _doc_categories_cache, _cache_timestamp
    
    # Si la caché es válida, devolver los datos en caché
    if _doc_categories_cache is not None and _is_cache_valid():
        return _doc_categories_cache
    
    try:
        # Importar aquí para evitar dependencias circulares
        from common.db_connector import execute_query
        
        query = """
        SELECT cb.id_categoria_bancaria, cb.nombre_categoria, cb.requiere_validacion,
               cb.validez_en_dias, td.id_tipo_documento, td.nombre_tipo
        FROM categorias_bancarias cb
        JOIN tipos_documento_bancario tdb ON cb.id_categoria_bancaria = tdb.id_categoria_bancaria
        JOIN tipos_documento td ON tdb.id_tipo_documento = td.id_tipo_documento
        """
        
        results = execute_query(query)
        category_map = {}
        doc_to_category_map = {}
        
        # Crear mapa de categorías y tipos de documento a categorías
        for row in results:
            category_id = row['id_categoria_bancaria']
            doc_type_id = row['id_tipo_documento']
            
            # Normalizar nombre de tipo de documento
            doc_type_name = row['nombre_tipo'].lower().replace(' ', '_')
            
            # Mapear tipo de documento a categoría
            doc_to_category_map[doc_type_name] = category_id
            doc_to_category_map[doc_type_id] = category_id
            
            # Guardar información de la categoría
            if category_id not in category_map:
                category_map[category_id] = {
                    'nombre': row['nombre_categoria'],
                    'requiere_validacion': bool(row['requiere_validacion']),
                    'validez_en_dias': row['validez_en_dias']
                }
        
        # Actualizar caché
        _doc_categories_cache = (category_map, doc_to_category_map)
        _cache_timestamp = datetime.now()
        
        return category_map, doc_to_category_map
    except Exception as e:
        logger.error(f"Error al obtener categorías bancarias: {str(e)}")
        # Si hay un error, devolver mapas por defecto
        default_category_map = {
            '5aeea247-26f4-11f0-8066-0affc7b8197b': {'nombre': 'Identificación', 'requiere_validacion': True, 'validez_en_dias': 3650},
            '5aeea5a8-26f4-11f0-8066-0affc7b8197b': {'nombre': 'Financiero', 'requiere_validacion': True, 'validez_en_dias': 365}
        }
        default_doc_to_category_map = {
            'dni': '5aeea247-26f4-11f0-8066-0affc7b8197b',
            'pasaporte': '5aeea247-26f4-11f0-8066-0affc7b8197b'
        }
        return default_category_map, default_doc_to_category_map

def get_validation_requirements():
    """Obtiene los requisitos de validación para cada categoría"""
    global _validation_requirements_cache, _cache_timestamp
    
    # Si la caché es válida, devolver los datos en caché
    if _validation_requirements_cache is not None and _is_cache_valid():
        return _validation_requirements_cache
    
    try:
        category_map, _ = get_document_categories_map()
        
        # Crear mapa de requisitos de validación
        validation_map = {}
        for category_id, info in category_map.items():
            validation_map[category_id] = info['requiere_validacion']
        
        # Actualizar caché
        _validation_requirements_cache = validation_map
        _cache_timestamp = datetime.now()
        
        return validation_map
    except Exception as e:
        logger.error(f"Error al obtener requisitos de validación: {str(e)}")
        # Si hay un error, devolver un mapa por defecto
        default_map = {
            '5aeea247-26f4-11f0-8066-0affc7b8197b': True,  # Identificación
            '5aeea5a8-26f4-11f0-8066-0affc7b8197b': True,  # Financiero
            '5aeea794-26f4-11f0-8066-0affc7b8197b': True,  # Contractual
            '5aeea896-26f4-11f0-8066-0affc7b8197b': True,  # Cumplimiento
            '5aeea9d1-26f4-11f0-8066-0affc7b8197b': True,  # Garantías
            '5aeeaaae-26f4-11f0-8066-0affc7b8197b': False, # Comunicaciones
            '5aeeab85-26f4-11f0-8066-0affc7b8197b': True,  # Fiscal
            '5aeeac66-26f4-11f0-8066-0affc7b8197b': True   # Laboral
        }
        return default_map

def determine_document_type(metadata, filename):
    """
    Determina el tipo de documento basado en metadatos y nombre de archivo.
    Consulta la base de datos para mapear a los tipos bancarios.
    """
    # Obtener mapas de la base de datos
    try:
        doc_type_map = get_document_types_map()
    except Exception as e:
        logger.error(f"Error al obtener tipos de documento: {str(e)}")
        # Valor por defecto en caso de error (ID del tipo 'Contrato cuenta')
        return '6b2b13cb-26f4-11f0-8066-0affc7b8197b'
    
    # Intentar determinar por nombre de archivo
    filename_lower = filename.lower()
    
    # Palabras clave comunes para tipos de documentos
    filename_hints = {
        'dni': ['dni', 'identidad', 'id'],
        'pasaporte': ['pasaporte', 'passport'],
        'contrato_cuenta': ['contrato', 'cuenta', 'apertura'],
        'nomina': ['nomina', 'salario', 'payroll'],
        'formulario_kyc': ['kyc', 'conoce', 'cliente', 'blanqueo'],
        'extracto_bancario': ['extracto', 'estado', 'cuenta', 'bancario'],
        'comprobante_domicilio': ['domicilio', 'residencia', 'dirección'],
        'declaracion_impuestos': ['impuesto', 'fiscal', 'tributaria', 'irpf']
    }
    
    # Buscar coincidencias en el nombre del archivo
    for doc_type_key, keywords in filename_hints.items():
        for keyword in keywords:
            if keyword in filename_lower:
                # Devolver el UUID del tipo de documento si existe en el mapa
                if doc_type_key in doc_type_map:
                    return doc_type_map[doc_type_key]
    
    # Si no se pudo determinar por nombre, usar extensión
    extension = metadata.get('extension', '').lower()
    if extension == 'pdf':
        # Por defecto para PDFs, buscar tipo de contrato
        for key in ['contrato_cuenta', 'contrato', 'ccb']:
            if key in doc_type_map:
                return doc_type_map[key]
    elif extension in ['jpg', 'jpeg', 'png']:
        # Por defecto para imágenes, buscar documento de identidad
        for key in ['dni', 'id']:
            if key in doc_type_map:
                return doc_type_map[key]
    
    # Si todo falla, devolver el primer tipo de documento
    return list(doc_type_map.values())[0] if doc_type_map else '6b2b13cb-26f4-11f0-8066-0affc7b8197b'

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

def guess_document_type(text):
    """
    Intenta determinar el tipo de documento basado en el texto extraído.
    Utiliza análisis de patrones, palabras clave y evaluación de confianza mejorada.
    """
    if not text:
        return {
            'document_type': 'desconocido',
            'document_type_id': None,
            'confidence': 0.0,
            'scores': {}
        }
    
    # Normalizar texto
    text_lower = text.lower()
    
    # Inicializamos puntuaciones para cada tipo
    scores = {
        'dni': 0,
        'pasaporte': 0,
        'contrato_cuenta': 0,
        'contrato_tarjeta': 0,
        'extracto_bancario': 0,
        'nomina': 0,
        'declaracion_impuestos': 0,
        'formulario_kyc': 0,
        'comprobante_domicilio': 0
    }
    
    # Buscar patrones específicos con ponderación
    if re.search(DNI_PATTERN, text):
        scores['dni'] += 50
    
    if re.search(PASSPORT_PATTERN, text):
        scores['pasaporte'] += 50
    
    if re.search(IBAN_PATTERN, text):
        scores['contrato_cuenta'] += 20
        scores['extracto_bancario'] += 30
    
    if re.search(CREDIT_CARD_PATTERN, text):
        scores['contrato_tarjeta'] += 40
        scores['contrato_cuenta'] += 10
    
    # Patrones adicionales para mejorar la detección
    fecha_patterns = {
        'expedicion': r'(?i)fecha\s+de\s+(?:expedición|emisión|alta)[\s:]+(\d{1,2}[/.-]\d{1,2}[/.-]\d{2,4})',
        'caducidad': r'(?i)(?:fecha\s+de\s+(?:caducidad|validez)|válido\s+hasta)[\s:]+(\d{1,2}[/.-]\d{1,2}[/.-]\d{2,4})'
    }
    
    # Si encontramos fechas de expedición y caducidad, probablemente es un documento de identidad
    exp_fecha = re.search(fecha_patterns['expedicion'], text)
    cad_fecha = re.search(fecha_patterns['caducidad'], text)
    
    if exp_fecha and cad_fecha:
        scores['dni'] += 30
        scores['pasaporte'] += 30
    
    # Buscar patrones específicos para extractos bancarios
    if re.search(r'(?i)saldo\s+(?:anterior|inicial)[\s:]+[\d.,]+', text) and \
       re.search(r'(?i)saldo\s+(?:final|nuevo|actual)[\s:]+[\d.,]+', text):
        scores['extracto_bancario'] += 50
    
    # Buscar tablas que indiquen movimientos bancarios
    movimientos_pattern = r'(?i)(?:fecha|concepto|importe|cargo|abono)'
    movimientos_matches = len(re.findall(movimientos_pattern, text_lower))
    if movimientos_matches >= 3:
        scores['extracto_bancario'] += 40
    
    # Patrones específicos para nóminas
    if re.search(r'(?i)(?:salario\s+base|salario\s+bruto)[\s:]+[\d.,]+', text) and \
       re.search(r'(?i)(?:líquido\s+a\s+percibir|total\s+neto)[\s:]+[\d.,]+', text):
        scores['nomina'] += 60
    
    # Palabras clave con pesos por relevancia
    keyword_map = {
        'dni': [('documento nacional', 10), ('identidad', 5), ('dni', 15), 
                ('dirección general de policía', 10), ('apellidos y nombre', 8)],
        'pasaporte': [('pasaporte', 15), ('passport', 15), ('travel document', 10), 
                      ('nacionalidad', 5), ('autoridad/authority', 8)],
        'contrato_cuenta': [('contrato', 5), ('cuenta', 8), ('condiciones', 3), 
                           ('titular', 5), ('entidad', 3), ('interés', 3), ('comisiones', 8)],
        'contrato_tarjeta': [('tarjeta', 10), ('crédito', 8), ('débito', 8), 
                            ('pin', 5), ('límite', 5), ('disposición', 3)],
        'extracto_bancario': [('extracto', 15), ('saldo', 10), ('fecha valor', 8), 
                             ('movimientos', 8), ('periodo', 5), ('cuenta', 3)],
        'nomina': [('nómina', 15), ('salario', 10), ('retribución', 8), ('irpf', 8), 
                  ('seguridad social', 10), ('empresa', 3), ('trabajador', 3)],
        'declaracion_impuestos': [('declaración', 5), ('impuesto', 10), ('irpf', 15), 
                                 ('hacienda', 8), ('fiscal', 8), ('tributaria', 8), ('renta', 10)],
        'formulario_kyc': [('conocimiento', 5), ('cliente', 3), ('kyc', 15), 
                          ('diligencia', 5), ('prevención', 5), ('blanqueo', 10), ('pbc', 15)],
        'comprobante_domicilio': [('certificado', 5), ('empadronamiento', 15), ('domicilio', 10), 
                                ('residencia', 10), ('suministro', 8), ('factura', 5)]
    }
    
    # Contar apariciones de palabras clave ponderadas
    for doc_type, keywords in keyword_map.items():
        for keyword, weight in keywords:
            count = text_lower.count(keyword)
            if count > 0:
                scores[doc_type] += count * weight
    
    # Determinar el tipo con mayor puntuación
    max_score = 0
    document_type = 'desconocido'
    
    for doc_type, score in scores.items():
        if score > max_score:
            max_score = score
            document_type = doc_type
    
    # Cálculo de confianza mejorado
    confidence = 0.0
    if max_score > 0:
        # Rangos de confianza según puntuación:
        # 0-20: baja confianza (0.1-0.5)
        # 20-50: confianza media (0.5-0.75)
        # 50+: alta confianza (0.75-0.95)
        if max_score < 20:
            confidence = 0.1 + (0.4 * (max_score / 20))
        elif max_score < 50:
            confidence = 0.5 + (0.25 * ((max_score - 20) / 30))
        else:
            confidence = 0.75 + (0.2 * min((max_score - 50) / 100, 1.0))
    
    # Verificar diferencia con la segunda puntuación más alta para ajustar confianza
    scores_list = [(t, s) for t, s in scores.items() if t != document_type]
    if scores_list:
        scores_list.sort(key=lambda x: x[1], reverse=True)
        second_highest = scores_list[0][1] if scores_list else 0
        diff_ratio = (max_score - second_highest) / max(max_score, 1)
        
        # Si la diferencia es pequeña, reducir confianza
        if diff_ratio < 0.3 and max_score > 0:
            confidence *= 0.8  # Penalización por ambigüedad
    
    # Limitar confianza al rango 0.0-0.95
    confidence = min(0.95, max(0.0, confidence))
    
    # Mapear el tipo detectado a un ID de base de datos
    doc_type_map = get_document_types_map()
    document_type_id = doc_type_map.get(document_type, None)
    
    logger.info(f"Tipo de documento identificado: {document_type} con confianza {confidence:.2f}")
    
    return {
        'document_type': document_type,
        'document_type_id': document_type_id,
        'confidence': confidence,
        'scores': scores
    }

def validate_id_document(extracted_data):
    """Valida un documento de identidad extraído"""
    validation = {
        'valid': True,
        'errors': [],
        'warnings': []
    }
    
    # Verificar campos obligatorios
    required_fields = ['numero_identificacion', 'nombre_completo', 'fecha_expiracion']
    
    for field in required_fields:
        if field not in extracted_data or not extracted_data[field]:
            validation['valid'] = False
            validation['errors'].append(f"Falta el campo obligatorio: {field}")
    
    # Validar formato de número de identificación
    if 'numero_identificacion' in extracted_data and extracted_data['numero_identificacion']:
        numero = extracted_data['numero_identificacion']
        if not (re.match(DNI_PATTERN, numero) or re.match(PASSPORT_PATTERN, numero)):
            validation['warnings'].append("El formato del número de identificación parece incorrecto")
    
    return validation

def get_document_expiry_info(document_type_id):
    """
    Obtiene información sobre la caducidad de un tipo de documento.
    Retorna (requiere_validacion, validez_en_dias)
    """
    try:
        # Obtener categorías y mapa de tipos a categorías
        category_map, doc_to_category_map = get_document_categories_map()
        
        # Buscar la categoría del tipo de documento
        if document_type_id in doc_to_category_map:
            category_id = doc_to_category_map[document_type_id]
            
            # Obtener información de la categoría
            if category_id in category_map:
                cat_info = category_map[category_id]
                return cat_info['requiere_validacion'], cat_info['validez_en_dias']
    
    except Exception as e:
        logger.error(f"Error al obtener información de caducidad: {str(e)}")
    
    # Por defecto, asumir que requiere validación sin caducidad definida
    return True, None