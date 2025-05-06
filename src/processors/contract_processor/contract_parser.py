# src/processors/contract_processor/contract_parser.py
import os
import re
import json
import logging
from datetime import datetime

logger = logging.getLogger()
logger.setLevel(os.environ.get('LOG_LEVEL', 'INFO'))

# Patrones para extraer información de contratos
ACCOUNT_NUMBER_PATTERN = r'(?i)(?:Cuenta|Nº de Cuenta|Account Number)[^\d]*(\d{4}[\s-]?\d{4}[\s-]?\d{4}[\s-]?\d{4})'
IBAN_PATTERN = r'(?i)(?:IBAN)[^\w]*([A-Z]{2}\d{2}[A-Z0-9]{4}\d{7}(?:[A-Z0-9]{0,16}))'
CONTRACT_NUMBER_PATTERN = r'(?i)(?:Contrato|Nº de Contrato|Número de Contrato|Contract Number)[^\d]*([A-Z0-9]{5,20})'
START_DATE_PATTERN = r'(?i)(?:Fecha de inicio|Inicio vigencia|Start date)[^\d]*(\d{1,2}[/-]\d{1,2}[/-]\d{2,4})'
END_DATE_PATTERN = r'(?i)(?:Fecha de fin|Fin vigencia|Vencimiento|End date)[^\d]*(\d{1,2}[/-]\d{1,2}[/-]\d{2,4})'
INTEREST_RATE_PATTERN = r'(?i)(?:Tipo de interés|Interés|Interest rate|TAE)[^\d]*(\d{1,2}(?:[.,]\d{1,4})?)\s*%'
AMOUNT_PATTERN = r'(?i)(?:Importe|Valor del contrato|Principal|Amount)[^\d]*(\d{1,3}(?:[.,]\d{3})*(?:[.,]\d{2}))\s*(?:€|\$|EUR|USD|euros?|dólares?)'
SIGNATORY_PATTERN = r'(?i)(?:Firmado por|Firma|Signed by)[^\w]*([\w\s.,]+)'
CONTRACT_TYPE_PATTERNS = {
    'cuenta_corriente': [r'cuenta corriente', r'cuenta de depósito', r'checking account'],
    'cuenta_ahorro': [r'cuenta de ahorro', r'libreta', r'savings account'],
    'deposito': [r'depósito a plazo', r'depósito fijo', r'term deposit', r'fixed deposit'],
    'prestamo': [r'préstamo personal', r'crédito personal', r'personal loan'],
    'hipoteca': [r'préstamo hipotecario', r'hipoteca', r'mortgage', r'loan mortgage'],
    'tarjeta_credito': [r'tarjeta de crédito', r'credit card'],
    'inversion': [r'fondo de inversión', r'plan de inversión', r'investment fund'],
    'seguro': [r'póliza de seguro', r'seguro', r'insurance policy']
}

def extract_contract_data(text, text_blocks):
    """Extrae información específica de contratos bancarios"""
    # Resultado de la extracción
    extracted_data = {
        'tipo_contrato': 'otro',
        'numero_contrato': None,
        'fecha_inicio': None,
        'fecha_fin': None,
        'estado': 'pendiente_firma',
        'valor_contrato': None,
        'tasa_interes': None,
        'periodo_tasa': 'anual',
        'moneda': 'EUR',
        'numero_producto': None,
        'firmado_digitalmente': False,
        'firmantes': [],
        'clausulas_importantes': [],
        'texto_completo': text
    }
    
    # Determinar tipo de contrato
    for tipo, patrones in CONTRACT_TYPE_PATTERNS.items():
        for patron in patrones:
            if re.search(patron, text, re.IGNORECASE):
                extracted_data['tipo_contrato'] = tipo
                break
        if extracted_data['tipo_contrato'] != 'otro':
            break
    
    # Extraer número de contrato
    contract_number_match = re.search(CONTRACT_NUMBER_PATTERN, text, re.IGNORECASE)
    if contract_number_match:
        extracted_data['numero_contrato'] = contract_number_match.group(1).strip()
    
    # Extraer IBAN o número de cuenta como número de producto
    iban_match = re.search(IBAN_PATTERN, text, re.IGNORECASE)
    if iban_match:
        extracted_data['numero_producto'] = iban_match.group(1).strip()
    else:
        account_match = re.search(ACCOUNT_NUMBER_PATTERN, text, re.IGNORECASE)
        if account_match:
            extracted_data['numero_producto'] = account_match.group(1).strip()
    
    # Extraer fechas
    start_date_match = re.search(START_DATE_PATTERN, text, re.IGNORECASE)
    if start_date_match:
        extracted_data['fecha_inicio'] = start_date_match.group(1).strip()
    
    end_date_match = re.search(END_DATE_PATTERN, text, re.IGNORECASE)
    if end_date_match:
        extracted_data['fecha_fin'] = end_date_match.group(1).strip()
    
    # Extraer tipo de interés
    interest_match = re.search(INTEREST_RATE_PATTERN, text, re.IGNORECASE)
    if interest_match:
        try:
            extracted_data['tasa_interes'] = float(interest_match.group(1).replace(',', '.'))
        except ValueError:
            logger.warning(f"No se pudo convertir la tasa de interés: {interest_match.group(1)}")
    
    # Extraer importe
    amount_match = re.search(AMOUNT_PATTERN, text, re.IGNORECASE)
    if amount_match:
        try:
            amount_str = amount_match.group(1).replace('.', '').replace(',', '.')
            extracted_data['valor_contrato'] = float(amount_str)
        except ValueError:
            logger.warning(f"No se pudo convertir el importe: {amount_match.group(1)}")
    
    # Extraer firmantes
    signatory_matches = re.finditer(SIGNATORY_PATTERN, text, re.IGNORECASE)
    for match in signatory_matches:
        firmante = match.group(1).strip()
        if firmante and firmante not in extracted_data['firmantes']:
            extracted_data['firmantes'].append(firmante)
    
    # Determinar estado
    if len(extracted_data['firmantes']) > 0:
        extracted_data['estado'] = 'vigente'
        extracted_data['firmado_digitalmente'] = bool(re.search(r'firma\s+digital|firma\s+electrónica', text, re.IGNORECASE))
    
    # Extraer cláusulas importantes (fragmentos que contienen información legal relevante)
    important_clauses_keywords = [
        r'resolución anticipada',
        r'cancelación',
        r'penalización',
        r'comisiones?',
        r'modificación',
        r'garantías?',
        r'avales?',
        r'incumplimiento',
        r'ley aplicable',
        r'jurisdicción'
    ]
    
    # Buscar bloques de texto que puedan contener cláusulas importantes
    for block in text_blocks:
        for keyword in important_clauses_keywords:
            if re.search(keyword, block, re.IGNORECASE):
                clause = block.strip()
                if clause and clause not in extracted_data['clausulas_importantes']:
                    extracted_data['clausulas_importantes'].append(clause)
                break
    
    return extracted_data

def validate_contract_data(extracted_data):
    """Valida los datos extraídos de un contrato bancario"""
    validation = {
        'is_valid': True,
        'confidence': 0.0,
        'errors': [],
        'warnings': []
    }
    
    # Verificar campos obligatorios
    if 'numero_contrato' not in extracted_data or not extracted_data['numero_contrato']:
        validation['warnings'].append("No se ha podido extraer el número de contrato")

    if 'fecha_inicio' not in extracted_data or not extracted_data['fecha_inicio']:
        validation['warnings'].append("No se ha podido extraer la fecha de inicio")

    if 'tipo_contrato' not in extracted_data or extracted_data['tipo_contrato'] == 'otro':
        validation['warnings'].append("No se ha podido determinar el tipo de contrato")

    # Para contratos de préstamo e hipoteca, verificar importe y tasa de interés
    if extracted_data.get('tipo_contrato') in ['prestamo', 'hipoteca']:
        if 'valor_contrato' not in extracted_data or not extracted_data['valor_contrato']:
            validation['warnings'].append("No se ha podido extraer el importe del préstamo")
        
        if 'tasa_interes' not in extracted_data or not extracted_data['tasa_interes']:
            validation['warnings'].append("No se ha podido extraer la tasa de interés")

    # Para cuentas y tarjetas, verificar número de producto
    if extracted_data.get('tipo_contrato') in ['cuenta_corriente', 'cuenta_ahorro', 'tarjeta_credito']:
        if 'numero_producto' not in extracted_data or not extracted_data['numero_producto']:
            validation['warnings'].append("No se ha podido extraer el número de cuenta o tarjeta")
    
    # Calcular confianza basada en campos extraídos correctamente
    # La lista de campos cambia según el tipo de contrato
    required_fields = ['numero_contrato', 'fecha_inicio']
    
    if extracted_data['tipo_contrato'] in ['prestamo', 'hipoteca']:
        required_fields.extend(['valor_contrato', 'tasa_interes'])
    
    if extracted_data['tipo_contrato'] in ['cuenta_corriente', 'cuenta_ahorro', 'tarjeta_credito']:
        required_fields.append('numero_producto')
    
    extracted_fields = sum(1 for field in required_fields if extracted_data.get(field))
    validation['confidence'] = extracted_fields / len(required_fields)
    
    # Si hay demasiadas advertencias, marcar como no válido
    if len(validation['warnings']) > 3:
        validation['is_valid'] = False
        validation['errors'].append("Demasiada información importante no pudo ser extraída")
    
    return validation

def format_date(date_str):
    """Convierte una fecha en formato string a formato ISO"""
    if not date_str:
        return None
    
    # Patrones comunes de fecha
    patterns = [
        r'(\d{1,2})[/-](\d{1,2})[/-](\d{4})',  # DD/MM/YYYY o DD-MM-YYYY
        r'(\d{1,2})[/-](\d{1,2})[/-](\d{2})',   # DD/MM/YY o DD-MM-YY
        r'(\d{4})[/-](\d{1,2})[/-](\d{1,2})'    # YYYY/MM/DD o YYYY-MM-DD
    ]
    
    for pattern in patterns:
        match = re.match(pattern, date_str)
        if match:
            groups = match.groups()
            if len(groups[2]) == 4:  # Si el año tiene 4 dígitos
                if len(groups) == 3:
                    # Formato DD/MM/YYYY
                    return f"{groups[2]}-{groups[1].zfill(2)}-{groups[0].zfill(2)}"
            elif len(groups[2]) == 2:  # Si el año tiene 2 dígitos
                year = int(groups[2])
                if year < 50:  # Asumimos que años < 50 son del siglo XXI
                    year += 2000
                else:  # Años >= 50 son del siglo XX
                    year += 1900
                # Formato DD/MM/YY
                return f"{year}-{groups[1].zfill(2)}-{groups[0].zfill(2)}"
            elif len(groups[0]) == 4:  # YYYY/MM/DD
                return f"{groups[0]}-{groups[1].zfill(2)}-{groups[2].zfill(2)}"
    
    # Si no se reconoce el formato, devolver None
    return None

def extract_clauses_by_section(text):
    """Extrae cláusulas del contrato agrupadas por secciones"""
    clauses = {}
    
    # Patrones para identificar inicios de sección
    section_patterns = [
        (r'CONDICIONES GENERALES', 'condiciones_generales'),
        (r'CONDICIONES PARTICULARES', 'condiciones_particulares'),
        (r'CLÁUSULAS', 'clausulas'),
        (r'COMISIONES', 'comisiones'),
        (r'INTERESES', 'intereses'),
        (r'GARANTÍAS', 'garantias'),
        (r'RESOLUCIÓN', 'resolucion')
    ]
    
    # Texto completo en minúsculas para buscar secciones
    text_lower = text.lower()
    
    # Buscar cada sección
    for pattern, section_key in section_patterns:
        pattern_lower = pattern.lower()
        if pattern_lower in text_lower:
            # Encontrar el inicio de la sección
            start_idx = text_lower.find(pattern_lower)
            
            # Buscar el inicio de la siguiente sección
            end_idx = len(text)
            for next_pattern, _ in section_patterns:
                next_pattern_lower = next_pattern.lower()
                next_idx = text_lower.find(next_pattern_lower, start_idx + len(pattern_lower))
                if next_idx > start_idx and next_idx < end_idx:
                    end_idx = next_idx
            
            # Extraer el texto de la sección
            section_text = text[start_idx:end_idx].strip()
            
            # Guardar la sección
            clauses[section_key] = section_text
    
    return clauses