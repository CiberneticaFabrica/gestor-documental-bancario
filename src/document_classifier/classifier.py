# src/document_classifier/classifier.py
import os
import logging
import re
import json
from urllib.parse import unquote_plus

# Configuración del logger
logger = logging.getLogger()
logger.setLevel(os.environ.get('LOG_LEVEL', 'INFO'))

# Patrones para tipos de documentos bancarios
DOCUMENT_PATTERNS = {
    'dni': {
        'keywords': [
            'documento nacional de identidad', 'dni', 'identidad', 'república',
            'cédula', 'cedula', 'tribunal electoral', 'panama', 'panamá',
            'identificación', 'identification', 'id card', 'número personal',
            'fecha de nacimiento', 'lugar de nacimiento', 'expedida', 'expira',
            'sexo', 'tipo de sangre'
        ],
        'regex': [
            r'(?i)DNI\s*[-:]?\s*\d{8}[A-Z]?', 
            r'(?i)IDENTIDAD\s*N[º°]?\s*\d+',
            r'(?i)\d{1,2}-\d{3,4}-\d{1,4}',  # Formato cédula panameña: X-XXX-XXXX o XX-XXX-XXXX
            r'(?i)CEDULA\s*N[º°.]?\s*\d+',
            r'(?i)EXPEDIDA:.*EXPIRA',  # Patrón común en cédulas
            r'(?i)TRIBUNAL\s*ELECTORAL',  # Específico para cédulas panameñas
            r'(?i)REPUBLICA\s+DE\s+PANAMA',  # Específico para cédulas panameñas
            r'(?i)FECHA\s+DE\s+NACIMIENTO', # Común en documentos de identidad
            r'(?i)(NOMBRE|APELLIDOS?)(:|\.|\s)+[A-ZÁÉÍÓÚÄËÏÖÜÑÇÀÈÌÒÙa-záéíóúäëïöüñçàèìòù\s]+',  # Línea de nombre en documentos de identidad
            r'(?i)(NACIONALIDAD|NATIONALITY)(:|\.|\s)+[A-ZÁÉÍÓÚÄËÏÖÜÑÇÀÈÌÒÙa-záéíóúäëïöüñçàèìòù\s]+'  # Nacionalidad en documentos
        ]
    },
    'pasaporte': {
        'keywords': [
            'pasaporte', 'passport', 'république', 'kingdom', 'república',
            'nationality', 'nacionalidad', 'type', 'tipo', 'autoridad',
            'authority', 'date of issue', 'date of expiry', 'place of birth',
            'fecha de expedición', 'fecha de expiración', 'lugar de nacimiento'
        ],
        'regex': [
            r'(?i)PASAPORTE\s*N[º°]?\s*[A-Z0-9]+', 
            r'(?i)PASSPORT\s*NO?\.?\s*[A-Z0-9]+',
            r'(?i)P[A-Z]{3}.*\d+',  # Formato típico de pasaportes: PXXX12345...
            r'(?i)(DATE\s+OF\s+ISSUE|DATE\s+OF\s+EXPIRY)',
            r'(?i)(SURNAME|GIVEN\s+NAMES|NOM|PRENOM)',
            r'(?i)(TYPE|CODE|AUTHORITY)',
            r'(?i)(PASSPORT|PASAPORTE)\s+(NO|NUM|NÚMERO|NUMBER)',
            r'(?i)(NATIONALITY|NACIONALIDAD|NATIONALITÉ)'
        ]
    },
    'nomina': {
        'keywords': ['nómina', 'salario', 'retribución', 'sueldo', 'payroll'],
        'regex': [r'(?i)NOMINA\s*(DEL|DE)\s*(MES)?\s*[A-Za-z]+', r'(?i)PERIODO\s*[A-Za-z]+\s*\d{4}']
    },
    'extracto_bancario': {
        'keywords': ['extracto', 'cuenta', 'saldo', 'movimientos', 'statement'],
        'regex': [r'(?i)EXTRACTO\s*(DE)?\s*CUENTA', r'(?i)SALDO\s*ANTERIOR\s*[\d.,]+']
    },
    'contrato': {
        'keywords': ['contrato', 'acuerdo', 'estipulaciones', 'cláusulas', 'partes'],
        'regex': [r'(?i)CONTRATO\s*DE\s*[A-Za-z\s]+', r'(?i)REUNIDOS|COMPARECEN|EXPONEN']
    },
    'factura': {
        'keywords': ['factura', 'invoice', 'impuesto', 'iva', 'importe'],
        'regex': [r'(?i)FACTURA\s*N[º°]?\s*[A-Z0-9-]+', r'(?i)FECHA\s*(DE)?\s*EMISI[OÓ]N\s*\d{1,2}[/-]\d{1,2}[/-]\d{2,4}']
    },
    'recibo': {
        'keywords': ['recibo', 'pago', 'receipt', 'cobro'],
        'regex': [r'(?i)RECIBO\s*N[º°]?\s*\d+', r'(?i)PAGO\s*(POR|DE)\s*[A-Za-z\s]+']
    },
    'formulario_kyc': {
        'keywords': ['conocimiento', 'cliente', 'kyc', 'know your customer', 'prevención', 'blanqueo'],
        'regex': [r'(?i)FORMULARIO\s*(DE)?\s*CONOCIMIENTO\s*(DEL)?\s*CLIENTE', r'(?i)KYC']
    }
}

# Mapa de tipos de documentos a IDs en base de datos
# Esto debería obtenerse de la base de datos en implementación real
DOCUMENT_TYPE_IDS = {
    'dni': '6b2b0c6b-26f4-11f0-8066-0affc7b8197b',  # ID para documentos de identidad DNI
    'pasaporte': '6b2b1196-26f4-11f0-8066-0affc7b8197b',  # ID para pasaportes
    'nomina': '6b2b1516-26f4-11f0-8066-0affc7b8197b',  # ID para nóminas
    'extracto_bancario': 'd4e5f6g7-4567-8901-abcd-ef123456789012',  # ID para extractos
    'contrato': 'e5f6g7h8-5678-9012-abcd-ef1234567890123',  # ID para contratos
    'factura': 'f6g7h8i9-6789-0123-abcd-ef12345678901234',  # ID para facturas
    'recibo': 'g7h8i9j0-7890-1234-abcd-ef12345678901234',  # ID para recibos
    'formulario_kyc': '6b2b163f-26f4-11f0-8066-0affc7b8197b',  # ID para formularios KYC
    'desconocido': 'd5f2cac1-3308-4a1d-9a65-e7b1a3fa3a8d',  # ID genérico para documentos no clasificados
    'Comprobante_domicilio': '6b2b1888-26f4-11f0-8066-0affc7b8197b'  # ID Comprobante de domicilio
}

# Patrones específicos para cédulas panameñas y documentos de identidad
PANAMA_CEDULA_PATTERNS = [
    r'(?i)REPUBLICA DE PANAMA.*TRIBUNAL ELECTORAL',
    r'(?i)TRIBUNAL ELECTORAL.*PANAMA',
    r'(?i)FECHA DE NACIMIENTO.*LUGAR DE NACIMIENTO',
    r'(?i)EXPEDIDA.*EXPIRA',
    r'(?i)SEXO.*TIPO DE SANGRE'
]

# Patrones generales para documentos de identidad de cualquier país
GENERAL_ID_PATTERNS = [
    r'(?i)(FECHA|DATE)\s+(DE)?\s+(NACIMIENTO|BIRTH)',
    r'(?i)(NACIONALIDAD|NATIONALITY)',
    r'(?i)(NUMERO|NUMBER|N.\s+DE)\s+(IDENTIDAD|ID|IDENTIFICATION)',
    r'(?i)(NOMBRE|NAME).*?(APELLIDO|SURNAME|LAST NAME)',
    r'(?i)(EXPEDICION|EXPIRACION|ISSUE|EXPIRY)'
]

def detect_cedula_format(text):
    """Detecta formatos típicos de cédulas panameñas u otros documentos de identidad"""
    # Formato típico de cédula panameña: X-XXX-XXXX o XX-XXX-XXXX
    panama_cedula_matches = re.findall(r'\d{1,2}-\d{3,4}-\d{1,4}', text)
    
    # Otros formatos de documento de identidad (números largos, formatos con letras)
    other_id_matches = re.findall(r'(?i)(DNI|ID|CEDULA|C.C.|DUI)[\s:.#-]*([A-Z0-9]{5,})', text)
    
    return panama_cedula_matches, other_id_matches

def check_specific_id_patterns(text):
    """Verifica patrones muy específicos para documentos de identidad"""
    # Patrones específicos de cédulas panameñas
    panama_score = 0
    for pattern in PANAMA_CEDULA_PATTERNS:
        if re.search(pattern, text):
            panama_score += 3  # Alto peso para estos patrones específicos
    
    # Patrones generales de documentos de identidad
    general_id_score = 0
    for pattern in GENERAL_ID_PATTERNS:
        if re.search(pattern, text):
            general_id_score += 2
    
    return panama_score, general_id_score

def classify_document(text, structured_data=None):
    """
    Clasifica el documento según el texto extraído.
    Mejorado para detectar documentos de identidad, especialmente cédulas panameñas.
    """
    try:
        # Inicializar puntuación para cada tipo de documento
        scores = {doc_type: 0 for doc_type in DOCUMENT_PATTERNS.keys()}
        
        # Normalizar texto (limpiar espacios extra, normalizar caracteres especiales)
        normalized_text = ' '.join(text.lower().split())
        
        # Analizar por palabras clave con distintos pesos según la ubicación
        # Las palabras al inicio del documento tienen más peso
        first_500_chars = normalized_text[:500]
        
        # Verificar si hay patrones específicos de cédulas panameñas u otros IDs
        panama_cedula_matches, other_id_matches = detect_cedula_format(text)
        panama_score, general_id_score = check_specific_id_patterns(text)
        
        # Si encontramos patrones fuertes de cédula panameña, aumentar puntuación
        if panama_cedula_matches or panama_score >= 3:
            scores['dni'] += 5  # Alto peso para cédulas panameñas
            logger.info(f"Detectado posible cédula panameña: {panama_cedula_matches}")
        
        # Si encontramos otros formatos de ID, aumentar puntuación general de DNI
        if other_id_matches or general_id_score >= 2:
            scores['dni'] += 3
            logger.info(f"Detectado posible documento de identidad general")
        
        for doc_type, patterns in DOCUMENT_PATTERNS.items():
            # Puntuación por palabras clave generales (en todo el texto)
            for keyword in patterns['keywords']:
                if keyword.lower() in normalized_text:
                    scores[doc_type] += 1
                    # Mayor peso si aparece al inicio del documento
                    if keyword.lower() in first_500_chars:
                        scores[doc_type] += 2
            
            # Puntuación por patrones regex con diferentes pesos
            for regex_pattern in patterns['regex']:
                try:
                    matches = re.findall(regex_pattern, text)
                    base_weight = 2  # Peso base para coincidencias regex
                    
                    # Más peso para patrones muy específicos 
                    if any(term in regex_pattern.upper() for term in ['DNI', 'IDENTIDAD', 'PASAPORTE', 'CEDULA', 'TRIBUNAL']):
                        base_weight = 3
                        
                    scores[doc_type] += len(matches) * base_weight
                except Exception as regex_error:
                    logger.warning(f"Error en patrón regex '{regex_pattern}': {str(regex_error)}")
        
        # Si hay datos estructurados disponibles, usarlos
        if structured_data:
            # Verificar teléfonos para cédulas panameñas (mal clasificadas)
            if 'phone' in structured_data and structured_data['phone']:
                for phone in structured_data['phone']:
                    # Verificar si tiene formato de cédula panameña (X-XXX-XXXX)
                    if re.match(r'\d{1,2}-\d{3,4}-\d{1,4}', phone):
                        scores['dni'] += 5  # Alto peso para cédulas panameñas
                        logger.info(f"Detectado número en formato de cédula panameña: {phone}")
            
            # Ejemplo: Si se detectaron DNIs y el tipo es dni o pasaporte, aumentar puntuación
            if 'dni' in structured_data and structured_data['dni']:
                scores['dni'] += 3
            
            if 'passport' in structured_data and structured_data['passport']:
                scores['pasaporte'] += 3
                
            # Ejemplo: Si hay muchas fechas, probablemente sea un extracto o contrato
            if 'dates' in structured_data and len(structured_data['dates']) > 2:
                scores['extracto_bancario'] += 1
                scores['contrato'] += 1
                
            # Ejemplo: Si hay cantidades monetarias, probablemente sea una factura, nómina o extracto
            if 'amounts' in structured_data and structured_data['amounts']:
                scores['factura'] += 1
                scores['nomina'] += 1
                scores['extracto_bancario'] += 1
        
        # Log para diagnóstico
        logger.info(f"Puntuaciones finales: {scores}")
        
        # Determinar el tipo con mayor puntuación
        best_type = max(scores.items(), key=lambda x: x[1])
        
        # Si la puntuación es demasiado baja, clasificar como contrato (tipo más común)
        if best_type[1] < 3:
            doc_type = 'contrato'
            confidence = 0.5
        else:
            doc_type = best_type[0]
            # Calcular confianza (normalizada entre 0 y 1)
            # Modificada para dar más peso al margen entre el mejor y el segundo mejor
            sorted_scores = sorted(scores.values(), reverse=True)
            margin = sorted_scores[0] - sorted_scores[1] if len(sorted_scores) > 1 else sorted_scores[0]
            
            # La confianza depende tanto del valor absoluto como del margen sobre el segundo
            confidence = min(0.95, max(0.4, (best_type[1] * 0.7 + margin * 0.3) / 10))
        
        return {
            'document_type': doc_type,
            'document_type_id': DOCUMENT_TYPE_IDS.get(doc_type, DOCUMENT_TYPE_IDS['contrato']),
            'confidence': confidence,
            'scores': scores
        }
    except Exception as e:
        logger.error(f"Error en classify_document: {str(e)}")
        # En caso de error, devolver clasificación por defecto
        return {
            'document_type': 'contrato',
            'document_type_id': DOCUMENT_TYPE_IDS['contrato'],
            'confidence': 0.5,
            'scores': {'contrato': 1}
        }

def classify_by_filename(filename):
    """Clasificación de respaldo basada en el nombre del archivo"""
    try:
        # Decodificar nombre de archivo si está codificado en URL
        filename = unquote_plus(filename)
        filename_lower = filename.lower()
        
        # Patrones para diferentes tipos de documentos
        if any(kw in filename_lower for kw in ['dni', 'documento', 'identidad', 'cedula', 'ced', 'panama', 'personal']):
            return {
                'document_type': 'dni',
                'document_type_id': DOCUMENT_TYPE_IDS['dni'],
                'confidence': 0.7,
                'scores': {'dni': 3}
            }
        elif any(kw in filename_lower for kw in ['pasaporte', 'passport']):
            return {
                'document_type': 'pasaporte',
                'document_type_id': DOCUMENT_TYPE_IDS['pasaporte'],
                'confidence': 0.7,
                'scores': {'pasaporte': 3}
            }
        elif any(kw in filename_lower for kw in ['contrato', 'contract']):
            return {
                'document_type': 'contrato',
                'document_type_id': DOCUMENT_TYPE_IDS['contrato'],
                'confidence': 0.7,
                'scores': {'contrato': 3}
            }
        elif any(kw in filename_lower for kw in ['extracto', 'estado cuenta', 'account', 'statement']):
            return {
                'document_type': 'extracto_bancario',
                'document_type_id': DOCUMENT_TYPE_IDS['extracto_bancario'],
                'confidence': 0.6,
                'scores': {'extracto_bancario': 2}
            }
        elif any(kw in filename_lower for kw in ['nomina', 'payroll', 'salario']):
            return {
                'document_type': 'nomina',
                'document_type_id': DOCUMENT_TYPE_IDS['nomina'],
                'confidence': 0.6,
                'scores': {'nomina': 2}
            }
        elif any(kw in filename_lower for kw in ['factura', 'invoice']):
            return {
                'document_type': 'factura',
                'document_type_id': DOCUMENT_TYPE_IDS['factura'],
                'confidence': 0.6,
                'scores': {'factura': 2}
            }
        elif any(kw in filename_lower for kw in ['recibo', 'receipt']):
            return {
                'document_type': 'recibo',
                'document_type_id': DOCUMENT_TYPE_IDS['recibo'],
                'confidence': 0.6,
                'scores': {'recibo': 2}
            }
        elif any(kw in filename_lower for kw in ['kyc', 'cliente', 'customer']):
            return {
                'document_type': 'formulario_kyc',
                'document_type_id': DOCUMENT_TYPE_IDS['formulario_kyc'],
                'confidence': 0.6,
                'scores': {'formulario_kyc': 2}
            }
        
        # Por defecto, considerar como contrato con baja confianza
        return {
            'document_type': 'contrato',
            'document_type_id': DOCUMENT_TYPE_IDS['contrato'],
            'confidence': 0.5,
            'scores': {'contrato': 1}
        }
    except Exception as e:
        logger.error(f"Error en classify_by_filename: {str(e)}")
        # En caso de error, devolver clasificación por defecto
        return {
            'document_type': 'contrato',
            'document_type_id': DOCUMENT_TYPE_IDS['contrato'],
            'confidence': 0.5,
            'scores': {'contrato': 1}
        }