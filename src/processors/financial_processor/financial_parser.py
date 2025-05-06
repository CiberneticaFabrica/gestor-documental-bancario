# src/processors/financial_processor/financial_parser.py
import re
import json
from datetime import datetime

def parse_financial_document(texto, entities):
    """
    Parsea documentos financieros como extractos bancarios
    
    Args:
        texto (str): Texto extraído del documento
        entities (dict): Entidades detectadas organizadas por tipo
    
    Returns:
        dict: Datos financieros extraídos
    """
    financial_data = {}
    
    # Normalizar texto para búsquedas en minúsculas
    texto_lower = texto.lower()
    
    # Extraer número de cuenta (IBAN)
    iban_pattern = r'\b[A-Z]{2}\d{2}[A-Z0-9]{4}[A-Z0-9]{4}[A-Z0-9]{4}[A-Z0-9]{4}(?:[A-Z0-9]{4})?\b'
    iban_match = re.search(iban_pattern, texto)
    if iban_match:
        financial_data['numero_cuenta'] = iban_match.group(0)
    
    # Buscar otros formatos de cuenta (no IBAN)
    if not 'numero_cuenta' in financial_data:
        account_pattern = r'(?:cuenta|nº cuenta|account)[^\d]{1,20}(\d{4}[\s-]?\d{4}[\s-]?\d{4}[\s-]?\d{4})'
        account_match = re.search(account_pattern, texto_lower)
        if account_match:
            financial_data['numero_cuenta'] = account_match.group(1).replace(' ', '').replace('-', '')
    
    # Extraer tipo de documento financiero
    financial_data['tipo_documento'] = determine_financial_document_type(texto_lower)
    
    # Extraer saldo actual
    saldo_patterns = [
        r'(?:saldo|balance)(?:\s+actual|\s+final|\s+disponible)?(?:\s*:|\s+es|\s+de)?\s+(?:EUR|\€)?(?:\s*)([0-9.,]+)(?:\s*\€|EUR)?',
        r'(?:total|disponible)(?:\s*:|\s+es|\s+de)?\s+(?:EUR|\€)?(?:\s*)([0-9.,]+)(?:\s*\€|EUR)?',
        r'(?:EUR|\€)\s*([0-9.,]+)'
    ]
    
    for pattern in saldo_patterns:
        saldo_match = re.search(pattern, texto_lower)
        if saldo_match:
            financial_data['saldo'] = saldo_match.group(1).replace('.', '').replace(',', '.')
            break
    
    # Extraer fecha del extracto
    fecha_patterns = [
        r'fecha(?:\s+extracto|\s+estado)?(?:\s*:|\s+del|\s+de)?\s+(\d{1,2}[/.-]\d{1,2}[/.-]\d{2,4})',
        r'extracto(?:\s+al|\s+del)?\s+(\d{1,2}[/.-]\d{1,2}[/.-]\d{2,4})',
        r'(?:statement date|fecha documento)(?:\s*:)?\s+(\d{1,2}[/.-]\d{1,2}[/.-]\d{2,4})'
    ]
    
    for pattern in fecha_patterns:
        fecha_match = re.search(pattern, texto_lower)
        if fecha_match:
            financial_data['fecha_extracto'] = fecha_match.group(1)
            break
    
    # Extraer periodo del extracto
    periodo_match = re.search(r'periodo(?:\s*:|\s+del)?\s+(\d{1,2}[/.-]\d{1,2}[/.-]\d{2,4})(?:\s+al|\s*-\s*)(\d{1,2}[/.-]\d{1,2}[/.-]\d{2,4})', texto_lower)
    if periodo_match:
        financial_data['periodo_inicio'] = periodo_match.group(1)
        financial_data['periodo_fin'] = periodo_match.group(2)
    
    # Extraer entidad bancaria desde entidades de organización
    if 'ORGANIZATION' in entities:
        # Filtrar solo entidades que parezcan bancos
        bancos = [e for e in entities['ORGANIZATION'] if is_bank_name(e)]
        if bancos:
            financial_data['entidad_bancaria'] = bancos[0]
    
    # Extraer titular de la cuenta
    if 'PERSON' in entities:
        financial_data['titular'] = entities['PERSON'][0]
    
    # Extraer moneda
    moneda_patterns = [
        r'(?:moneda|divisa|currency)[^\w]{1,10}((?:EUR|USD|GBP|JPY|CHF|euros?|dólares?|francos?|libras?|yenes?))',
        r'importes? en ((?:EUR|USD|GBP|JPY|CHF|euros?|dólares?|francos?|libras?|yenes?))'
    ]
    
    for pattern in moneda_patterns:
        moneda_match = re.search(pattern, texto_lower)
        if moneda_match:
            moneda = moneda_match.group(1).upper()
            if 'EURO' in moneda:
                financial_data['moneda'] = 'EUR'
            elif 'DOLAR' in moneda or 'USD' in moneda:
                financial_data['moneda'] = 'USD'
            elif 'LIBRA' in moneda or 'GBP' in moneda:
                financial_data['moneda'] = 'GBP'
            elif 'YEN' in moneda or 'JPY' in moneda:
                financial_data['moneda'] = 'JPY'
            elif 'FRANCO' in moneda or 'CHF' in moneda:
                financial_data['moneda'] = 'CHF'
            else:
                financial_data['moneda'] = moneda
            break
    
    # Si no se detectó moneda, asumir EUR para bancos españoles
    if 'moneda' not in financial_data:
        financial_data['moneda'] = 'EUR'
    
    # Extracción de información adicional específica del tipo de documento
    if financial_data['tipo_documento'] == 'extracto_bancario':
        # Para extractos bancarios, buscar información de intereses
        intereses_pattern = r'(?:intereses|interests)(?:\s+abonados|\s+acumulados|\s+netos)?(?:\s*:|\s+es|\s+de|\s+por)?\s+([0-9.,]+)'
        intereses_match = re.search(intereses_pattern, texto_lower)
        if intereses_match:
            financial_data['intereses'] = intereses_match.group(1).replace('.', '').replace(',', '.')
    
    elif financial_data['tipo_documento'] == 'recibo':
        # Para recibos, buscar información específica
        emisor_pattern = r'(?:emisor|pagado a|beneficiario)(?:\s*:|\s+es|\s+de)?\s+([A-Za-z\s.,]+)'
        emisor_match = re.search(emisor_pattern, texto_lower)
        if emisor_match:
            financial_data['emisor'] = emisor_match.group(1).strip()
        
        concepto_pattern = r'(?:concepto|motivo|descripción)(?:\s*:|\s+es|\s+de)?\s+([A-Za-z0-9\s.,]+)'
        concepto_match = re.search(concepto_pattern, texto_lower)
        if concepto_match:
            financial_data['concepto_pago'] = concepto_match.group(1).strip()
    
    return financial_data

def extract_transactions(texto):
    """
    Extrae movimientos bancarios del texto del extracto
    
    Args:
        texto (str): Texto del extracto bancario
    
    Returns:
        list: Lista de transacciones
    """
    # Buscar patrones de líneas de transacciones
    transactions = []
    
    # Dividir el texto en líneas
    lines = texto.split('\n')
    
    # Patrones para detectar líneas de transacciones
    # Patrón 1: fecha + texto descriptivo + importe (posiblemente con signo)
    pattern1 = r'(\d{1,2}[/.-]\d{1,2}(?:[/.-]\d{2,4})?)(?:\s+)([^0-9€$]{3,50})(?:\s+)([-+]?\d+[,.]\d{2})'
    
    # Patrón 2: fecha + referencia + texto descriptivo + importe
    pattern2 = r'(\d{1,2}[/.-]\d{1,2})(?:\s+)(\w+)(?:\s+)([^0-9€$]{3,50})(?:\s+)([-+]?\d+[,.]\d{2})'
    
    # Combinamos los resultados de ambos patrones
    for line in lines:
        # Intentar con el primer patrón
        match1 = re.search(pattern1, line)
        if match1:
            fecha = match1.group(1)
            concepto = match1.group(2).strip()
            importe = match1.group(3).replace('.', '').replace(',', '.')
            
            if concepto and importe:
                transaction = {
                    'fecha': fecha,
                    'concepto': concepto,
                    'importe': importe
                }
                transactions.append(transaction)
                continue  # Si encontramos con el patrón 1, pasamos a la siguiente línea
        
        # Si no funciona, intentar con el segundo patrón
        match2 = re.search(pattern2, line)
        if match2:
            fecha = match2.group(1)
            referencia = match2.group(2).strip()
            concepto = match2.group(3).strip()
            importe = match2.group(4).replace('.', '').replace(',', '.')
            
            if concepto and importe:
                transaction = {
                    'fecha': fecha,
                    'referencia': referencia,
                    'concepto': concepto,
                    'importe': importe
                }
                transactions.append(transaction)
    
    return transactions

def is_bank_name(name):
    """
    Determina si el nombre parece ser de una entidad bancaria
    
    Args:
        name (str): Nombre de la organización
    
    Returns:
        bool: True si parece un banco, False en caso contrario
    """
    bank_keywords = ['banco', 'bank', 'caja', 'credit', 'crédito', 'santander', 'bbva', 
                    'sabadell', 'popular', 'caixabank', 'bankia', 'kutxa', 'cajamar', 
                    'ibercaja', 'bankinter', 'unicaja', 'abanca', 'deutsche', 'bnp', 
                    'barclays', 'citibank', 'hsbc', 'ing', 'openbank']
    
    name_lower = name.lower()
    
    for keyword in bank_keywords:
        if keyword in name_lower:
            return True
    
    return False

def determine_financial_document_type(texto_lower):
    """
    Determina el tipo específico de documento financiero
    
    Args:
        texto_lower (str): Texto del documento en minúsculas
    
    Returns:
        str: Tipo de documento financiero
    """
    # Palabras clave para cada tipo de documento
    keywords = {
        'extracto_bancario': ['extracto', 'estado de cuenta', 'account statement', 'movimientos', 'saldo anterior', 'saldo final'],
        'recibo': ['recibo', 'receipt', 'comprobante de pago', 'pago realizado'],
        'factura': ['factura', 'invoice', 'importe a pagar', 'total a pagar', 'base imponible', 'iva'],
        'nomina': ['nómina', 'payroll', 'salario', 'retribución', 'devengos', 'retenciones', 'irpf']
    }
    
    # Contar ocurrencias de cada tipo
    counts = {doc_type: sum(1 for kw in kws if kw in texto_lower) for doc_type, kws in keywords.items()}
    
    # Devolver el tipo con más coincidencias
    if counts:
        max_type = max(counts.items(), key=lambda x: x[1])
        if max_type[1] > 0:  # Si hay al menos una coincidencia
            return max_type[0]
    
    # Si no hay coincidencias claras, revisar contenido
    if 'movimiento' in texto_lower or 'saldo' in texto_lower:
        return 'extracto_bancario'
    elif 'factura' in texto_lower or 'iva' in texto_lower:
        return 'factura'
    
    # Por defecto, asumimos extracto bancario
    return 'extracto_bancario'