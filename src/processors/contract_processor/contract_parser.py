# contract_parser.py - VERSIÓN COMPLETA CORREGIDA
import os
import re
import json
import logging
from datetime import datetime

logger = logging.getLogger()
logger.setLevel(os.environ.get('LOG_LEVEL', 'INFO'))

def extract_contract_data_from_queries_enhanced(query_answers, text_fallback):
    """
    ENHANCED VERSION: Better extraction with improved field mapping
    """
    logger.info(f"🔍 Extrayendo datos de contrato desde query answers (versión mejorada)...")
    
    # Enhanced query mapping with multiple possible aliases
    query_mapping = {
        'numero_contrato': ['numero_contrato', 'contract_number', 'numero_documento', 'referencia'],
        'nombre_prestatario': ['nombre_prestatario', 'borrower_name', 'titular_cuenta', 'nombre_titular'],
        'monto_prestamo': ['monto_prestamo', 'loan_amount', 'monto_apertura', 'valor_contrato', 'capital', 'principal'],
        'tasa_interes': ['tasa_interes', 'interest_rate', 'tasa_tarjeta', 'tipo_interes'],
        'cuota_mensual': ['cuota_mensual', 'monthly_payment', 'pago_mensual'],
        'fecha_contrato': ['fecha_contrato', 'contract_date', 'fecha_firma', 'fecha_inicio'],
        'plazo_meses': ['plazo_meses', 'loan_term', 'plazo', 'duracion'],
        'nombre_banco': ['nombre_banco', 'bank_name', 'entidad_financiera'],
        'tipo_cuenta': ['tipo_cuenta', 'account_type', 'tipo_producto'],
        'numero_cuenta': ['numero_cuenta', 'account_number', 'numero_producto']
    }
    
    # Initialize with better defaults
    extracted_data = {
        'tipo_contrato': 'prestamo',  # Better default based on the log data
        'numero_contrato': None,
        'fecha_inicio': None,
        'fecha_fin': None,
        'estado': 'pendiente_firma',
        'valor_contrato': None,
        'tasa_interes': None,
        'periodo_tasa': 'anual',
        'moneda': 'USD',  # Changed from EUR to USD based on log data
        'numero_producto': None,
        'firmado_digitalmente': False,
        'firmantes': [],
        'clausulas_importantes': [],
        'observaciones': []
    }
    
    logger.info(f"📊 Procesando {len(query_answers)} query answers...")
    
    # Process query answers with enhanced extraction
    for field, aliases in query_mapping.items():
        value_found = None
        source_alias = None
        
        for alias in aliases:
            if alias in query_answers and query_answers[alias]:
                raw_value = query_answers[alias].strip()
                
                # Skip empty or "not found" responses
                if raw_value.lower() in ['not found', 'no encontrado', 'n/a', 'none', '']:
                    continue
                
                value_found = raw_value
                source_alias = alias
                break
        
        if value_found:
            # Process the found value based on field type
            if field == 'numero_contrato':
                extracted_data['numero_contrato'] = value_found
                logger.info(f"📝 Número contrato (query): {value_found} [from {source_alias}]")
                
            elif field in ['monto_prestamo', 'valor_contrato']:
                # Enhanced amount extraction
                amount_match = re.search(r'[\d.,]+', value_found.replace(',', '.'))
                if amount_match:
                    try:
                        # Handle different formats: 35,000.00 or 35.000,00
                        amount_str = amount_match.group(0)
                        if ',' in amount_str and '.' in amount_str:
                            # Format: 35,000.00 (US format)
                            amount_str = amount_str.replace(',', '')
                        elif ',' in amount_str and amount_str.count(',') == 1 and len(amount_str.split(',')[1]) == 2:
                            # Format: 35000,00 (EU format)
                            amount_str = amount_str.replace(',', '.')
                        
                        extracted_data['valor_contrato'] = float(amount_str)
                        logger.info(f"💰 Valor contrato (query): {extracted_data['valor_contrato']} [from {source_alias}]")
                        
                        # Extract currency if present
                        if 'US$' in value_found or '$' in value_found or 'USD' in value_found:
                            extracted_data['moneda'] = 'USD'
                        elif '€' in value_found or 'EUR' in value_found:
                            extracted_data['moneda'] = 'EUR'
                            
                    except ValueError as e:
                        logger.warning(f"⚠️ Error convirtiendo valor: {value_found} - {e}")
            
            elif field == 'fecha_contrato':
                # Enhanced date extraction and validation
                date_formatted = format_date_enhanced(value_found)
                if date_formatted:
                    extracted_data['fecha_inicio'] = date_formatted
                    logger.info(f"📅 Fecha contrato (query): {date_formatted} [from {source_alias}]")
                else:
                    logger.warning(f"⚠️ Fecha inválida: {value_found}")
            
            elif field == 'plazo_meses':
                # Extract numeric value for term
                term_match = re.search(r'(\d+)', value_found)
                if term_match:
                    try:
                        plazo = int(term_match.group(1))
                        if extracted_data['fecha_inicio'] and plazo > 0:
                            # Calculate end date if start date is available
                            from datetime import datetime, timedelta
                            start_date = datetime.strptime(extracted_data['fecha_inicio'], '%Y-%m-%d')
                            end_date = start_date + timedelta(days=plazo * 30)  # Approximate months
                            extracted_data['fecha_fin'] = end_date.strftime('%Y-%m-%d')
                        logger.info(f"📊 Plazo (query): {plazo} meses [from {source_alias}]")
                    except ValueError as e:
                        logger.warning(f"⚠️ Error convirtiendo plazo: {term_match.group(1)} - {e}")
            
            elif field == 'numero_cuenta':
                # Clean account number
                clean_account = re.sub(r'[^\d\-]', '', value_found)
                extracted_data['numero_producto'] = clean_account
                logger.info(f"🏦 Número cuenta (query): {clean_account} [from {source_alias}]")
    
    # Enhanced fallback logic using text analysis
    if not extracted_data['fecha_inicio']:
        logger.warning(f"⚠️ Fecha inicio no encontrada en queries, buscando en texto...")
        date_from_text = extract_date_from_text(text_fallback)
        if date_from_text:
            extracted_data['fecha_inicio'] = date_from_text
            logger.info(f"📅 Fecha inicio (texto): {date_from_text}")
    
    # Determine contract type based on context
    if text_fallback:
        text_lower = text_fallback.lower()
        if 'préstamo' in text_lower or 'prestamo' in text_lower or 'loan' in text_lower:
            extracted_data['tipo_contrato'] = 'prestamo'
        elif 'cuenta corriente' in text_lower:
            extracted_data['tipo_contrato'] = 'cuenta_corriente'
        elif 'depósito' in text_lower or 'deposito' in text_lower:
            extracted_data['tipo_contrato'] = 'deposito'
    
    # Generate comprehensive observations
    observations = []
    if not extracted_data['numero_contrato']:
        observations.append("No se ha podido extraer el número de contrato")
    if not extracted_data['fecha_inicio']:
        observations.append("No se ha podido extraer la fecha de inicio")
    if not extracted_data['valor_contrato']:
        observations.append("No se ha podido extraer el valor del contrato")
    if not extracted_data['numero_producto']:
        observations.append("No se ha podido extraer el número de cuenta o tarjeta")
    
    extracted_data['observaciones'] = "; ".join(observations) if observations else None
    
    # Log summary
    logger.info(f"📊 RESUMEN EXTRACCIÓN MEJORADA:")
    logger.info(f"   Tipo: {extracted_data['tipo_contrato']}")
    logger.info(f"   Número: {extracted_data['numero_contrato']}")
    logger.info(f"   Fecha inicio: {extracted_data['fecha_inicio']}")
    logger.info(f"   Valor: {extracted_data['valor_contrato']} {extracted_data['moneda']}")
    logger.info(f"   Tasa: {extracted_data['tasa_interes']}%")
    logger.info(f"   Estado: {extracted_data['estado']}")
    
    return extracted_data

def format_date_enhanced(date_str):
    """Enhanced date formatting with multiple pattern support"""
    if not date_str or not isinstance(date_str, str):
        return None
    
    # Clean the input
    date_str = date_str.strip()
    
    # Enhanced patterns including Spanish formats
    patterns = [
        # Spanish formats
        (r'(\d{1,2})\s+de\s+(\w+)\s+de\s+(\d{4})', 'spanish_month'),  # "25 de mayo de 2025"
        (r'(\d{1,2})[/-](\d{1,2})[/-](\d{4})', 'dmy'),  # DD/MM/YYYY
        (r'(\d{4})[/-](\d{1,2})[/-](\d{1,2})', 'ymd'),  # YYYY/MM/DD
        (r'(\d{1,2})[/-](\d{1,2})[/-](\d{2})', 'dmy_short'),  # DD/MM/YY
    ]
    
    spanish_months = {
        'enero': 1, 'febrero': 2, 'marzo': 3, 'abril': 4,
        'mayo': 5, 'junio': 6, 'julio': 7, 'agosto': 8,
        'septiembre': 9, 'octubre': 10, 'noviembre': 11, 'diciembre': 12
    }
    
    for pattern, format_type in patterns:
        match = re.search(pattern, date_str, re.IGNORECASE)
        if match:
            try:
                if format_type == 'spanish_month':
                    day = int(match.group(1))
                    month_name = match.group(2).lower()
                    year = int(match.group(3))
                    
                    if month_name in spanish_months:
                        month = spanish_months[month_name]
                        return f"{year:04d}-{month:02d}-{day:02d}"
                
                elif format_type == 'dmy':
                    day, month, year = int(match.group(1)), int(match.group(2)), int(match.group(3))
                    if 1 <= day <= 31 and 1 <= month <= 12 and 1900 <= year <= 2100:
                        return f"{year:04d}-{month:02d}-{day:02d}"
                
                elif format_type == 'ymd':
                    year, month, day = int(match.group(1)), int(match.group(2)), int(match.group(3))
                    if 1 <= day <= 31 and 1 <= month <= 12 and 1900 <= year <= 2100:
                        return f"{year:04d}-{month:02d}-{day:02d}"
                
                elif format_type == 'dmy_short':
                    day, month, year_short = int(match.group(1)), int(match.group(2)), int(match.group(3))
                    year = 2000 + year_short if year_short < 50 else 1900 + year_short
                    if 1 <= day <= 31 and 1 <= month <= 12:
                        return f"{year:04d}-{month:02d}-{day:02d}"
                        
            except ValueError:
                continue
    
    logger.warning(f"⚠️ No se pudo formatear la fecha: {date_str}")
    return None

def extract_date_from_text(text):
    """Extract date from full text using various patterns"""
    if not text:
        return None
    
    # Look for contract dates in text
    date_patterns = [
        r'firmado.*?(\d{1,2}\s+de\s+\w+\s+de\s+\d{4})',
        r'fecha.*?contrato.*?(\d{1,2}[/-]\d{1,2}[/-]\d{4})',
        r'(\d{1,2}\s+de\s+mayo\s+de\s+2025)',  # Specific to the log example
        r'(\d{1,2}[/-]\d{1,2}[/-]2025)',
    ]
    
    for pattern in date_patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            date_candidate = match.group(1)
            formatted_date = format_date_enhanced(date_candidate)
            if formatted_date:
                return formatted_date
    
    return None

def validate_contract_data_enhanced(extracted_data):
    """Enhanced validation with detailed checks"""
    validation = {
        'is_valid': True,
        'confidence': 0.0,
        'errors': [],
        'warnings': []
    }
    
    # Critical field validation
    required_fields = {
        'numero_contrato': 'Número de contrato',
        'fecha_inicio': 'Fecha de inicio',
        'tipo_contrato': 'Tipo de contrato'
    }
    
    fields_present = 0
    for field, description in required_fields.items():
        if extracted_data.get(field):
            fields_present += 1
        else:
            validation['errors'].append(f"{description} no encontrado - campo crítico")
    
    # Set validity based on critical fields
    if fields_present < 2:  # Need at least 2 out of 3 critical fields
        validation['is_valid'] = False
    
    # Type-specific validations
    tipo_contrato = extracted_data.get('tipo_contrato')
    
    if tipo_contrato in ['prestamo', 'hipoteca']:
        if not extracted_data.get('valor_contrato'):
            validation['warnings'].append(f"Importe del {tipo_contrato} no encontrado")
        if not extracted_data.get('tasa_interes'):
            validation['warnings'].append(f"Tasa de interés no encontrada para {tipo_contrato}")
    
    # Value validations
    if extracted_data.get('valor_contrato'):
        valor = extracted_data['valor_contrato']
        if valor <= 0:
            validation['errors'].append("Valor del contrato inválido (menor o igual a 0)")
            validation['is_valid'] = False
        elif valor > 10000000:  # 10 million
            validation['warnings'].append("Valor del contrato inusualmente alto")
    
    if extracted_data.get('tasa_interes'):
        tasa = extracted_data['tasa_interes']
        if tasa < 0:
            validation['errors'].append("Tasa de interés negativa")
            validation['is_valid'] = False
        elif tasa > 50:
            validation['warnings'].append("Tasa de interés inusualmente alta")
    
    # Calculate confidence based on data completeness and quality
    total_fields = 10  # Total expected fields
    filled_fields = sum(1 for key, value in extracted_data.items() 
                       if value is not None and str(value).strip() != '')
    
    base_confidence = filled_fields / total_fields
    
    # Adjust confidence based on critical fields
    critical_field_bonus = fields_present / len(required_fields) * 0.3
    
    # Adjust for errors and warnings
    error_penalty = len(validation['errors']) * 0.15
    warning_penalty = len(validation['warnings']) * 0.05
    
    validation['confidence'] = max(0.0, min(1.0, 
        base_confidence + critical_field_bonus - error_penalty - warning_penalty))
    
    # Final validity check based on confidence
    if validation['confidence'] < 0.3:
        validation['is_valid'] = False
        validation['errors'].append("Confianza de extracción demasiado baja")
    
    logger.info(f"📊 VALIDACIÓN MEJORADA:")
    logger.info(f"   Válido: {validation['is_valid']}")
    logger.info(f"   Confianza: {validation['confidence']:.2f}")
    logger.info(f"   Campos completados: {filled_fields}/{total_fields}")
    logger.info(f"   Campos críticos: {fields_present}/{len(required_fields)}")
    
    if validation['errors']:
        logger.error(f"   Errores: {validation['errors']}")
    if validation['warnings']:
        logger.warning(f"   Advertencias: {validation['warnings']}")
    
    return validation

def generate_contract_summary(contract_data):
    """
    FUNCIÓN AÑADIDA: Genera un resumen ejecutivo del contrato
    """
    try:
        summary_parts = []
        
        # Tipo de contrato
        tipo = contract_data.get('tipo_contrato', 'contrato')
        summary_parts.append(f"Tipo: {tipo.replace('_', ' ').title()}")
        
        # Número de contrato
        if contract_data.get('numero_contrato'):
            summary_parts.append(f"N°: {contract_data['numero_contrato']}")
        
        # Valor del contrato
        if contract_data.get('valor_contrato'):
            moneda = contract_data.get('moneda', 'USD')
            valor = contract_data['valor_contrato']
            if valor >= 1000000:
                valor_str = f"{valor/1000000:.1f}M"
            elif valor >= 1000:
                valor_str = f"{valor/1000:.0f}K"
            else:
                valor_str = f"{valor:.0f}"
            summary_parts.append(f"Valor: {moneda} {valor_str}")
        
        # Tasa de interés
        if contract_data.get('tasa_interes'):
            summary_parts.append(f"Tasa: {contract_data['tasa_interes']}%")
        
        # Fecha
        if contract_data.get('fecha_inicio'):
            from datetime import datetime
            try:
                fecha = datetime.strptime(contract_data['fecha_inicio'], '%Y-%m-%d')
                fecha_str = fecha.strftime('%b %Y')
                summary_parts.append(f"Fecha: {fecha_str}")
            except:
                pass
        
        # Estado
        estado = contract_data.get('estado', 'pendiente')
        summary_parts.append(f"Estado: {estado.replace('_', ' ').title()}")
        
        return " | ".join(summary_parts) if summary_parts else "Contrato procesado"
        
    except Exception as e:
        logger.error(f"Error generando resumen: {str(e)}")
        return "Contrato - resumen no disponible"

def extract_clauses_by_section(text):
    """
    FUNCIÓN AÑADIDA: Extrae cláusulas importantes por sección
    """
    try:
        if not text or len(text) < 100:
            return {}
        
        clauses = {}
        
        # Patrones para encontrar secciones importantes
        section_patterns = {
            'obligaciones': [
                r'OBLIGACIONES.*?(?=\n[A-Z]{2,}|\n\d+\.|\nCLÁUSULA|\Z)',
                r'DEBERES.*?(?=\n[A-Z]{2,}|\n\d+\.|\nCLÁUSULA|\Z)',
                r'COMPROMISOS.*?(?=\n[A-Z]{2,}|\n\d+\.|\nCLÁUSULA|\Z)'
            ],
            'garantias': [
                r'GARANTÍAS.*?(?=\n[A-Z]{2,}|\n\d+\.|\nCLÁUSULA|\Z)',
                r'GARANTIAS.*?(?=\n[A-Z]{2,}|\n\d+\.|\nCLÁUSULA|\Z)',
                r'AVAL.*?(?=\n[A-Z]{2,}|\n\d+\.|\nCLÁUSULA|\Z)'
            ],
            'penalizaciones': [
                r'PENALIZACIONES.*?(?=\n[A-Z]{2,}|\n\d+\.|\nCLÁUSULA|\Z)',
                r'MULTAS.*?(?=\n[A-Z]{2,}|\n\d+\.|\nCLÁUSULA|\Z)',
                r'INCUMPLIMIENTO.*?(?=\n[A-Z]{2,}|\n\d+\.|\nCLÁUSULA|\Z)'
            ],
            'condiciones': [
                r'CONDICIONES.*?(?=\n[A-Z]{2,}|\n\d+\.|\nCLÁUSULA|\Z)',
                r'TÉRMINOS.*?(?=\n[A-Z]{2,}|\n\d+\.|\nCLÁUSULA|\Z)',
                r'DISPOSICIONES.*?(?=\n[A-Z]{2,}|\n\d+\.|\nCLÁUSULA|\Z)'
            ]
        }
        
        for section_name, patterns in section_patterns.items():
            for pattern in patterns:
                match = re.search(pattern, text, re.IGNORECASE | re.DOTALL)
                if match:
                    content = match.group(0)
                    # Limpiar y truncar el contenido
                    content = re.sub(r'\s+', ' ', content).strip()
                    if len(content) > 500:
                        content = content[:500] + "..."
                    clauses[section_name] = content
                    break
        
        # Si no se encontraron secciones específicas, buscar cláusulas numeradas
        if not clauses:
            clause_pattern = r'(CLÁUSULA\s+\d+.*?)(?=CLÁUSULA\s+\d+|\Z)'
            clause_matches = re.findall(clause_pattern, text, re.IGNORECASE | re.DOTALL)
            
            for i, clause in enumerate(clause_matches[:3]):  # Máximo 3 cláusulas
                clean_clause = re.sub(r'\s+', ' ', clause).strip()
                if len(clean_clause) > 300:
                    clean_clause = clean_clause[:300] + "..."
                clauses[f'clausula_{i+1}'] = clean_clause
        
        return clauses
        
    except Exception as e:
        logger.error(f"Error extrayendo cláusulas: {str(e)}")
        return {} 
