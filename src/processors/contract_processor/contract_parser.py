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

# Mejorar la función extract_contract_data en contract_parser.py

def extract_contract_data(text, text_blocks):
    """Extrae información específica de contratos bancarios - VERSIÓN MEJORADA"""
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
        'observaciones': None,
        'texto_completo': text
    }
    
    # Normalizar texto para búsqueda
    text_upper = text.upper()
    
    # Determinar tipo de contrato con patrones mejorados
    for tipo, patrones in CONTRACT_TYPE_PATTERNS.items():
        for patron in patrones:
            if re.search(patron, text, re.IGNORECASE):
                extracted_data['tipo_contrato'] = tipo
                logger.info(f"📋 Tipo de contrato detectado: {tipo}")
                break
        if extracted_data['tipo_contrato'] != 'otro':
            break
    
    # Extraer número de contrato - patrones mejorados
    contract_patterns = [
        r'(?i)(?:Contrato|Contract)\s*(?:N[°º]?|Número|Number|#)[:\s]*([A-Z0-9\-\/]+)',
        r'(?i)(?:N[°º]?\s*de\s*Contrato)[:\s]*([A-Z0-9\-\/]+)',
        r'(?i)(?:Referencia|Reference|Ref\.?)[:\s]*([A-Z0-9\-\/]+)',
        r'(?i)(?:Expediente|File\s*Number)[:\s]*([A-Z0-9\-\/]+)'
    ]
    
    for pattern in contract_patterns:
        match = re.search(pattern, text)
        if match:
            extracted_data['numero_contrato'] = match.group(1).strip()
            logger.info(f"📝 Número de contrato encontrado: {extracted_data['numero_contrato']}")
            break
    
    # Si no se encontró número de contrato, generar uno temporal
    if not extracted_data['numero_contrato']:
        # Buscar cualquier código alfanumérico largo que pueda ser el número
        potential_contract = re.search(r'\b([A-Z]{2,3}[\-\/]?\d{6,})\b', text)
        if potential_contract:
            extracted_data['numero_contrato'] = potential_contract.group(1)
            logger.info(f"📝 Número de contrato inferido: {extracted_data['numero_contrato']}")
    
    # Extraer IBAN o número de cuenta como número de producto
    iban_match = re.search(IBAN_PATTERN, text, re.IGNORECASE)
    if iban_match:
        extracted_data['numero_producto'] = iban_match.group(1).strip()
        logger.info(f"🏦 IBAN encontrado: {extracted_data['numero_producto']}")
    else:
        account_match = re.search(ACCOUNT_NUMBER_PATTERN, text, re.IGNORECASE)
        if account_match:
            extracted_data['numero_producto'] = account_match.group(1).strip().replace(' ', '').replace('-', '')
            logger.info(f"🏦 Número de cuenta encontrado: {extracted_data['numero_producto']}")
    
    # Extraer fechas con patrones mejorados
    date_patterns = [
        (r'(?i)(?:Fecha\s*de\s*inicio|Start\s*date|Desde\s*el|Vigente\s*desde)[:\s]*(\d{1,2}[/-]\d{1,2}[/-]\d{2,4})', 'fecha_inicio'),
        (r'(?i)(?:Fecha\s*de\s*fin|End\s*date|Hasta\s*el|Vigente\s*hasta)[:\s]*(\d{1,2}[/-]\d{1,2}[/-]\d{2,4})', 'fecha_fin'),
        (r'(?i)(?:Vencimiento|Expiration|Caduca)[:\s]*(\d{1,2}[/-]\d{1,2}[/-]\d{2,4})', 'fecha_fin'),
        (r'(?i)(?:Firmado\s*el|Signed\s*on|Fecha\s*de\s*firma)[:\s]*(\d{1,2}[/-]\d{1,2}[/-]\d{2,4})', 'fecha_inicio')
    ]
    
    for pattern, field in date_patterns:
        match = re.search(pattern, text)
        if match and not extracted_data[field]:
            extracted_data[field] = match.group(1).strip()
            logger.info(f"📅 {field}: {extracted_data[field]}")
    
    # Extraer tipo de interés con patrones mejorados
    interest_patterns = [
        r'(?i)(?:Tipo\s*de\s*interés|Interest\s*rate|TAE|TIN)[:\s]*(\d{1,2}[.,]\d{1,4})\s*%',
        r'(?i)(\d{1,2}[.,]\d{1,4})\s*%\s*(?:anual|annual|TAE)',
        r'(?i)(?:interés|interest)[^\d]*(\d{1,2}[.,]\d{1,4})\s*%'
    ]
    
    for pattern in interest_patterns:
        match = re.search(pattern, text)
        if match:
            try:
                extracted_data['tasa_interes'] = float(match.group(1).replace(',', '.'))
                logger.info(f"💰 Tasa de interés: {extracted_data['tasa_interes']}%")
                break
            except ValueError:
                logger.warning(f"No se pudo convertir la tasa de interés: {match.group(1)}")
    
    # Extraer importe con patrones mejorados
    amount_patterns = [
        r'(?i)(?:Importe|Valor\s*del\s*contrato|Principal|Amount|Capital)[:\s]*([€$]?\s*\d{1,3}(?:[.,]\d{3})*(?:[.,]\d{2})?)\s*([€$]|EUR|USD|euros?|dólares?)?',
        r'([€$]\s*\d{1,3}(?:[.,]\d{3})*(?:[.,]\d{2})?)',
        r'(\d{1,3}(?:[.,]\d{3})*(?:[.,]\d{2})?)\s*(?:€|EUR|euros?)'
    ]
    
    for pattern in amount_patterns:
        match = re.search(pattern, text)
        if match:
            try:
                amount_str = match.group(1).replace('€', '').replace('$', '').strip()
                amount_str = amount_str.replace('.', '').replace(',', '.')
                extracted_data['valor_contrato'] = float(amount_str)
                logger.info(f"💵 Valor del contrato: {extracted_data['valor_contrato']}")
                
                # Detectar moneda
                if match.lastindex >= 2 and match.group(2):
                    currency = match.group(2)
                    if '$' in currency or 'USD' in currency or 'dólar' in currency.lower():
                        extracted_data['moneda'] = 'USD'
                    else:
                        extracted_data['moneda'] = 'EUR'
                break
            except ValueError:
                logger.warning(f"No se pudo convertir el importe: {match.group(1)}")
    
    # Extraer firmantes con patrones mejorados
    signatory_patterns = [
        r'(?i)(?:Firmado\s*por|Firma|Signed\s*by|El\s*titular)[:\s]*((?:[A-ZÁÉÍÓÚÑ][a-záéíóúñ]+\s*){2,4})',
        r'(?i)(?:Cliente|Customer|Titular)[:\s]*((?:[A-ZÁÉÍÓÚÑ][a-záéíóúñ]+\s*){2,4})',
        r'(?i)(?:Nombre\s*y\s*apellidos|Name)[:\s]*((?:[A-ZÁÉÍÓÚÑ][a-záéíóúñ]+\s*){2,4})'
    ]
    
    for pattern in signatory_patterns:
        matches = re.finditer(pattern, text)
        for match in matches:
            firmante = match.group(1).strip()
            # Validar que parece un nombre real
            if len(firmante) > 5 and len(firmante.split()) >= 2:
                if firmante not in extracted_data['firmantes']:
                    extracted_data['firmantes'].append(firmante)
                    logger.info(f"✍️ Firmante encontrado: {firmante}")
    
    # Determinar estado basado en indicadores
    if len(extracted_data['firmantes']) > 0:
        extracted_data['estado'] = 'vigente'
        extracted_data['firmado_digitalmente'] = bool(re.search(
            r'(?i)(firma\s*digital|firma\s*electrónica|digitally\s*signed|electronic\s*signature)', 
            text
        ))
        logger.info(f"📑 Estado: vigente, Firmado digitalmente: {extracted_data['firmado_digitalmente']}")
    
    # Detectar si el contrato está cancelado o vencido
    if re.search(r'(?i)(cancelad[oa]|cancelled|terminad[oa]|terminated)', text):
        extracted_data['estado'] = 'cancelado'
        logger.info(f"📑 Estado detectado: cancelado")
    elif re.search(r'(?i)(vencid[oa]|expired|caducad[oa])', text):
        extracted_data['estado'] = 'vencido'
        logger.info(f"📑 Estado detectado: vencido")
    elif re.search(r'(?i)(suspendid[oa]|suspended)', text):
        extracted_data['estado'] = 'suspendido'
        logger.info(f"📑 Estado detectado: suspendido")
    
    # Extraer período de tasa
    period_patterns = [
        (r'(?i)(anual|yearly|annual)', 'anual'),
        (r'(?i)(mensual|monthly)', 'mensual'),
        (r'(?i)(trimestral|quarterly)', 'trimestral'),
        (r'(?i)(semestral|semiannual)', 'semestral')
    ]
    
    for pattern, period in period_patterns:
        if re.search(pattern, text):
            extracted_data['periodo_tasa'] = period
            logger.info(f"📊 Período de tasa: {period}")
            break
    
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
        r'jurisdicción',
        r'protección de datos',
        r'confidencialidad',
        r'rescisión',
        r'renovación automática'
    ]
    
    # Buscar bloques de texto que puedan contener cláusulas importantes
    found_clauses = []
    for block in text_blocks:
        for keyword in important_clauses_keywords:
            if re.search(keyword, block, re.IGNORECASE):
                clause = block.strip()
                if clause and len(clause) > 20 and clause not in found_clauses:
                    found_clauses.append(clause)
                    extracted_data['clausulas_importantes'].append({
                        'keyword': keyword,
                        'text': clause[:500]  # Limitar longitud
                    })
                break
    
    # Generar observaciones basadas en la extracción
    observations = []
    
    if not extracted_data['numero_contrato']:
        observations.append("No se pudo extraer el número de contrato")
    
    if not extracted_data['fecha_inicio']:
        observations.append("No se pudo determinar la fecha de inicio")
    
    if extracted_data['tipo_contrato'] == 'otro':
        observations.append("Tipo de contrato no determinado específicamente")
    
    if len(extracted_data['firmantes']) == 0:
        observations.append("No se detectaron firmantes en el documento")
    
    if len(extracted_data['clausulas_importantes']) > 0:
        observations.append(f"Se detectaron {len(extracted_data['clausulas_importantes'])} cláusulas importantes")
    
    extracted_data['observaciones'] = "; ".join(observations) if observations else None
    
    # Log resumen de extracción
    logger.info("📊 RESUMEN DE EXTRACCIÓN DE CONTRATO:")
    logger.info(f"   Tipo: {extracted_data['tipo_contrato']}")
    logger.info(f"   Número: {extracted_data['numero_contrato']}")
    logger.info(f"   Fechas: {extracted_data['fecha_inicio']} - {extracted_data['fecha_fin']}")
    logger.info(f"   Valor: {extracted_data['valor_contrato']} {extracted_data['moneda']}")
    logger.info(f"   Estado: {extracted_data['estado']}")
    logger.info(f"   Firmantes: {len(extracted_data['firmantes'])}")
    
    return extracted_data

def validate_contract_data(extracted_data):
    """Valida los datos extraídos de un contrato bancario - VERSIÓN MEJORADA"""
    validation = {
        'is_valid': True,
        'confidence': 0.0,
        'errors': [],
        'warnings': []
    }
    
    # Verificar campos críticos
    if not extracted_data.get('numero_contrato'):
        validation['errors'].append("Número de contrato no encontrado - campo crítico")
        validation['is_valid'] = False
    
    if not extracted_data.get('fecha_inicio'):
        validation['errors'].append("Fecha de inicio no encontrada - campo crítico")
        validation['is_valid'] = False
    
    if extracted_data.get('tipo_contrato') == 'otro':
        validation['warnings'].append("No se pudo determinar el tipo específico de contrato")
    
    # Validaciones específicas por tipo de contrato
    tipo_contrato = extracted_data.get('tipo_contrato')
    
    # Para contratos de préstamo e hipoteca
    if tipo_contrato in ['prestamo', 'hipoteca']:
        if not extracted_data.get('valor_contrato'):
            validation['errors'].append(f"Importe del {tipo_contrato} no encontrado - campo crítico para este tipo")
            validation['is_valid'] = False
        
        if not extracted_data.get('tasa_interes'):
            validation['warnings'].append(f"Tasa de interés no encontrada para {tipo_contrato}")
        
        if not extracted_data.get('fecha_fin'):
            validation['warnings'].append(f"Fecha de vencimiento no encontrada para {tipo_contrato}")
    
    # Para cuentas y tarjetas
    if tipo_contrato in ['cuenta_corriente', 'cuenta_ahorro', 'tarjeta_credito']:
        if not extracted_data.get('numero_producto'):
            validation['warnings'].append(f"Número de cuenta/tarjeta no encontrado para {tipo_contrato}")
    
    # Para depósitos
    if tipo_contrato == 'deposito':
        if not extracted_data.get('valor_contrato'):
            validation['warnings'].append("Importe del depósito no encontrado")
        
        if not extracted_data.get('fecha_fin'):
            validation['warnings'].append("Fecha de vencimiento del depósito no encontrada")
        
        if not extracted_data.get('tasa_interes'):
            validation['warnings'].append("Tipo de interés del depósito no encontrado")
    
    # Validar coherencia de fechas
    if extracted_data.get('fecha_inicio') and extracted_data.get('fecha_fin'):
        fecha_inicio_iso = format_date(extracted_data['fecha_inicio'])
        fecha_fin_iso = format_date(extracted_data['fecha_fin'])
        
        if fecha_inicio_iso and fecha_fin_iso:
            try:
                from datetime import datetime
                inicio = datetime.strptime(fecha_inicio_iso, '%Y-%m-%d')
                fin = datetime.strptime(fecha_fin_iso, '%Y-%m-%d')
                
                if inicio >= fin:
                    validation['errors'].append("Fecha de inicio posterior o igual a fecha de fin")
                    validation['is_valid'] = False
                
                # Verificar si el contrato debería estar vencido
                if fin < datetime.now() and extracted_data['estado'] == 'vigente':
                    validation['warnings'].append("El contrato aparece como vigente pero la fecha de fin ya pasó")
                    
            except ValueError:
                validation['warnings'].append("Error al validar coherencia de fechas")
    
    # Validar firmantes
    if len(extracted_data.get('firmantes', [])) == 0:
        validation['warnings'].append("No se detectaron firmantes en el contrato")
    
    # Validar valores numéricos
    if extracted_data.get('valor_contrato'):
        if extracted_data['valor_contrato'] <= 0:
            validation['errors'].append("Valor del contrato inválido (menor o igual a 0)")
            validation['is_valid'] = False
        elif extracted_data['valor_contrato'] > 10000000:  # 10 millones
            validation['warnings'].append("Valor del contrato inusualmente alto")
    
    if extracted_data.get('tasa_interes'):
        if extracted_data['tasa_interes'] < 0:
            validation['errors'].append("Tasa de interés negativa")
            validation['is_valid'] = False
        elif extracted_data['tasa_interes'] > 50:
            validation['warnings'].append("Tasa de interés inusualmente alta")
    
    # Calcular confianza basada en completitud y validez
    # Campos base que siempre se evalúan
    base_fields = ['numero_contrato', 'fecha_inicio', 'tipo_contrato', 'estado']
    base_score = sum(1 for field in base_fields if extracted_data.get(field)) / len(base_fields)
    
    # Campos adicionales según tipo
    additional_fields = []
    if tipo_contrato in ['prestamo', 'hipoteca']:
        additional_fields = ['valor_contrato', 'tasa_interes', 'fecha_fin']
    elif tipo_contrato in ['cuenta_corriente', 'cuenta_ahorro', 'tarjeta_credito']:
        additional_fields = ['numero_producto']
    elif tipo_contrato == 'deposito':
        additional_fields = ['valor_contrato', 'tasa_interes', 'fecha_fin']
    
    if additional_fields:
        additional_score = sum(1 for field in additional_fields if extracted_data.get(field)) / len(additional_fields)
        validation['confidence'] = (base_score * 0.6) + (additional_score * 0.4)
    else:
        validation['confidence'] = base_score
    
    # Ajustar confianza por errores y advertencias
    validation['confidence'] -= len(validation['errors']) * 0.15
    validation['confidence'] -= len(validation['warnings']) * 0.05
    
    # Asegurar que la confianza esté entre 0 y 1
    validation['confidence'] = max(0.0, min(1.0, validation['confidence']))
    
    # Si la confianza es muy baja, marcar como no válido
    if validation['confidence'] < 0.3:
        validation['is_valid'] = False
        validation['errors'].append("Confianza de extracción demasiado baja")
    
    # Log de validación
    logger.info(f"📊 VALIDACIÓN DE CONTRATO:")
    logger.info(f"   Válido: {validation['is_valid']}")
    logger.info(f"   Confianza: {validation['confidence']:.2f}")
    if validation['errors']:
        logger.error(f"   Errores: {validation['errors']}")
    if validation['warnings']:
        logger.warning(f"   Advertencias: {validation['warnings']}")
    
    return validation

def extract_contract_metadata(text):
    """
    Extrae metadatos adicionales del contrato que pueden ser útiles
    """
    metadata = {
        'has_signatures': False,
        'has_seals': False,
        'has_notary': False,
        'has_witnesses': False,
        'language': 'es',  # Por defecto español
        'pages_estimated': 1,
        'complexity': 'standard'
    }
    
    # Detectar firmas
    if re.search(r'(?i)(firma|signature|firmado|signed)', text):
        metadata['has_signatures'] = True
    
    # Detectar sellos
    if re.search(r'(?i)(sello|seal|stamp)', text):
        metadata['has_seals'] = True
    
    # Detectar notario
    if re.search(r'(?i)(notario|notary|notarial|fedatario)', text):
        metadata['has_notary'] = True
    
    # Detectar testigos
    if re.search(r'(?i)(testigo|witness)', text):
        metadata['has_witnesses'] = True
    
    # Detectar idioma
    if re.search(r'\b(the|and|or|with|from|this|that)\b', text, re.IGNORECASE):
        if re.search(r'\b(el|la|los|las|y|o|con|de|este|esta)\b', text, re.IGNORECASE):
            metadata['language'] = 'bilingual'
        else:
            metadata['language'] = 'en'
    
    # Estimar páginas (muy aproximado)
    words = len(text.split())
    metadata['pages_estimated'] = max(1, words // 300)  # Aproximadamente 300 palabras por página
    
    # Determinar complejidad
    if metadata['has_notary'] or metadata['pages_estimated'] > 10:
        metadata['complexity'] = 'complex'
    elif metadata['pages_estimated'] > 5 or len(re.findall(r'(?i)cláusula', text)) > 10:
        metadata['complexity'] = 'medium'
    
    return metadata

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

def generate_contract_summary(contract_data):
    """
    Genera un resumen ejecutivo del contrato
    """
    summary = []
    
    # Tipo y número
    tipo_display = contract_data.get('tipo_contrato', 'desconocido').replace('_', ' ').title()
    if contract_data.get('numero_contrato'):
        summary.append(f"Contrato de {tipo_display} N° {contract_data['numero_contrato']}")
    else:
        summary.append(f"Contrato de {tipo_display}")
    
    # Valor y condiciones financieras
    if contract_data.get('valor_contrato'):
        valor_fmt = f"{contract_data['valor_contrato']:,.2f}"
        moneda = contract_data.get('moneda', 'EUR')
        summary.append(f"Importe: {valor_fmt} {moneda}")
    
    if contract_data.get('tasa_interes'):
        periodo = contract_data.get('periodo_tasa', 'anual')
        summary.append(f"Tasa: {contract_data['tasa_interes']}% {periodo}")
    
    # Vigencia
    if contract_data.get('fecha_inicio'):
        if contract_data.get('fecha_fin'):
            summary.append(f"Vigencia: {contract_data['fecha_inicio']} - {contract_data['fecha_fin']}")
        else:
            summary.append(f"Vigente desde: {contract_data['fecha_inicio']}")
    
    # Estado
    estado_display = contract_data.get('estado', 'desconocido').replace('_', ' ').title()
    summary.append(f"Estado: {estado_display}")
    
    # Firmantes
    if contract_data.get('firmantes'):
        if len(contract_data['firmantes']) == 1:
            summary.append(f"Firmante: {contract_data['firmantes'][0]}")
        else:
            summary.append(f"Firmantes: {len(contract_data['firmantes'])} personas")
    
    return " | ".join(summary)

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