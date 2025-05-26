# src/common_layer/python/common/pdf_utils.py
import os
import logging
import boto3
import json
import io
import time
import tempfile
import re
from datetime import datetime
from urllib.parse import unquote_plus
from concurrent.futures import ThreadPoolExecutor

# Importamos PyPDF2 para manejo de PDFs incompatibles con Textract
try:
    import PyPDF2
    PYPDF2_AVAILABLE = True
except ImportError:
    PYPDF2_AVAILABLE = False
    logging.warning("PyPDF2 no está disponible, la extracción fallback será limitada")

# Configuración del logger
logger = logging.getLogger()
logger.setLevel(os.environ.get('LOG_LEVEL', 'INFO'))

# Configuración de clientes con reintentos
from botocore.config import Config
retry_config = Config(
    retries={
        'max_attempts': 3,
        'mode': 'standard'
    }
)

s3_client = boto3.client('s3', config=retry_config)
textract_client = boto3.client('textract', config=retry_config)

def operation_with_retry(operation_func, max_retries=3, base_delay=0.5, **kwargs):
    """Función genérica para ejecutar operaciones con reintentos y backoff exponencial"""
    last_exception = None
    for attempt in range(max_retries):
        try:
            return operation_func(**kwargs)
        except Exception as e:
            last_exception = e
            delay = base_delay * (2 ** attempt)
            logger.warning(f"Reintento {attempt+1}/{max_retries} después de error: {str(e)}. Esperando {delay:.2f}s")
            time.sleep(delay)
    
    logger.error(f"Operación falló después de {max_retries} intentos. Último error: {str(last_exception)}")
    raise last_exception

def extract_text_with_pypdf2(bucket, key):
    """
    Extrae texto de un PDF usando PyPDF2 cuando Textract falla.
    Esta función descarga el PDF temporalmente y extrae su texto.
    """
    if not PYPDF2_AVAILABLE:
        return "ERROR: PyPDF2 no está disponible para la extracción alternativa de texto."
    
    try:
        logger.info(f"Iniciando extracción de texto con PyPDF2 para {bucket}/{key}")
        
        # Crear un archivo temporal para descargar el PDF
        with tempfile.NamedTemporaryFile(suffix='.pdf', delete=False) as temp_file:
            temp_path = temp_file.name
        
        try:
            # Descargar el archivo de S3
            logger.info(f"Descargando {bucket}/{key} a {temp_path}")
            s3_client.download_file(bucket, key, temp_path)
            
            # Abrir el PDF y extraer texto
            text_parts = []
            with open(temp_path, 'rb') as file:
                try:
                    # PyPDF2 ha cambiado su API en diferentes versiones, intentamos ambos enfoques
                    try:
                        # Para PyPDF2 ≥ 2.0
                        reader = PyPDF2.PdfReader(file)
                        for page_num in range(len(reader.pages)):
                            page = reader.pages[page_num]
                            text_parts.append(page.extract_text())
                    except AttributeError:
                        # Para PyPDF2 < 2.0
                        reader = PyPDF2.PdfFileReader(file)
                        for page_num in range(reader.numPages):
                            page = reader.getPage(page_num)
                            text_parts.append(page.extractText())
                            
                    # Unir todo el texto
                    full_text = "\n".join([part for part in text_parts if part])
                    logger.info(f"Texto extraído exitosamente con PyPDF2: {len(full_text)} caracteres")
                    
                    # Si no se pudo extraer texto significativo, informamos
                    if not full_text or len(full_text.strip()) < 20:
                        logger.warning("PyPDF2 extrajo texto, pero parece insuficiente o vacío")
                        return "ADVERTENCIA: Texto extraído por PyPDF2 parece insuficiente o vacío."
                    
                    return full_text
                    
                except Exception as pdf_error:
                    logger.error(f"Error al procesar PDF con PyPDF2: {str(pdf_error)}")
                    return f"ERROR: No se pudo extraer texto con PyPDF2. {str(pdf_error)}"
        finally:
            # Limpiar el archivo temporal
            if os.path.exists(temp_path):
                os.remove(temp_path)
                logger.info(f"Archivo temporal eliminado: {temp_path}")
    
    except Exception as e:
        logger.error(f"Error general en extract_text_with_pypdf2: {str(e)}")
        return f"ERROR: Fallo crítico en procesamiento PyPDF2: {str(e)}"

# ===== NUEVAS FUNCIONES PARA PROCESAMIENTO COMPLETO =====

def extract_structured_patterns_pypdf2(text):
    """
    🆕 Extrae patrones estructurados del texto usando PyPDF2 (similar a Textract)
    """
    try:
        patterns = {
            'dni': r'\b\d{8}[A-Za-z]?\b',
            'passport': r'\b[A-Z]{1,2}[0-9]{6,7}\b',
            'email': r'\b[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}\b',
            'phone': r'\b(?:\+\d{1,3}\s?)?\(?\d{1,4}\)?[\s.-]?\d{3}[\s.-]?\d{4}\b',
            'iban': r'\b[A-Z]{2}\d{2}[A-Z0-9]{4}\d{7}([A-Z0-9]?){0,16}\b',
            'dates': r'\b\d{1,2}[/.-]\d{1,2}[/.-]\d{2,4}\b',
            'amounts': r'\b\d{1,3}(?:[.,]\d{3})*(?:[.,]\d{2})?\s*(?:€|EUR|USD|\$)?\b',
            'percentages': r'\b\d{1,2}(?:[.,]\d{1,2})?\s*%\b',
            'contract_numbers': r'\b(?:contrato|contract|ref|referencia)[:\s#]*([A-Z0-9-]+)\b',
            'nif_cif': r'\b[A-Z]\d{7}[A-Z0-9]\b'
        }
        
        structured_data = {}
        for entity_type, pattern in patterns.items():
            matches = re.findall(pattern, text, re.IGNORECASE)
            if matches:
                # Limpiar duplicados manteniendo orden
                unique_matches = list(dict.fromkeys(matches))
                structured_data[entity_type] = unique_matches[:10]  # Limitar a 10 por tipo
        
        logger.debug(f"🔍 Patrones PyPDF2 extraídos: {list(structured_data.keys())}")
        return structured_data
        
    except Exception as e:
        logger.error(f"Error extrayendo patrones PyPDF2: {str(e)}")
        return {}

def extract_type_specific_data_pypdf2(text, doc_type):
    """
    🆕 Extrae datos específicos según tipo de documento con PyPDF2
    """
    try:
        if doc_type in ['dni', 'pasaporte', 'cedula', 'cedula_panama']:
            return extract_id_document_data_pypdf2(text, doc_type == 'pasaporte')
        elif doc_type in ['contrato', 'contrato_cuenta', 'contrato_tarjeta', 'contrato_prestamo']:
            return extract_contract_data_pypdf2(text)
        elif doc_type in ['extracto', 'extracto_bancario', 'nomina']:
            return extract_financial_data_pypdf2(text)
        else:
            return extract_generic_data_pypdf2(text)
            
    except Exception as e:
        logger.error(f"Error en extracción específica PyPDF2: {str(e)}")
        return {}

def extract_id_document_data_pypdf2(text, is_passport=False):
    """
    🆕 Extrae datos de documentos de identidad con PyPDF2
    """
    try:
        data = {
            'tipo_identificacion': 'pasaporte' if is_passport else 'dni',
            'numero_identificacion': None,
            'nombre_completo': None,
            'fecha_nacimiento': None,
            'fecha_emision': None,
            'fecha_expiracion': None,
            'pais_emision': 'España' if not is_passport else None,
            'genero': None,
            'lugar_nacimiento': None,
            'nacionalidad': None
        }
        
        # Buscar número de documento
        if is_passport:
            passport_patterns = [
                r'pasaporte[:\s]*([A-Z]{1,2}[0-9]{6,7})',
                r'passport[:\s]*([A-Z]{1,2}[0-9]{6,7})',
                r'\b([A-Z]{1,2}[0-9]{6,7})\b'
            ]
            for pattern in passport_patterns:
                match = re.search(pattern, text, re.IGNORECASE)
                if match:
                    data['numero_identificacion'] = match.group(1)
                    break
        else:
            dni_patterns = [
                r'dni[:\s]*(\d{8}[A-Za-z]?)',
                r'documento[:\s]*(\d{8}[A-Za-z]?)',
                r'cédula[:\s]*(\d{8}[A-Za-z]?)',
                r'\b(\d{8}[A-Za-z]?)\b'
            ]
            for pattern in dni_patterns:
                match = re.search(pattern, text, re.IGNORECASE)
                if match:
                    data['numero_identificacion'] = match.group(1)
                    break
        
        # Buscar nombre completo
        name_patterns = [
            r'nombre[:\s]+([A-ZÁÉÍÓÚÑ][a-záéíóúñ]+(?:\s+[A-ZÁÉÍÓÚÑ][a-záéíóúñ]+)*)',
            r'apellidos[:\s]+([A-ZÁÉÍÓÚÑ][a-záéíóúñ]+(?:\s+[A-ZÁÉÍÓÚÑ][a-záéíóúñ]+)*)',
            r'titular[:\s]+([A-ZÁÉÍÓÚÑ][a-záéíóúñ]+(?:\s+[A-ZÁÉÍÓÚÑ][a-záéíóúñ]+)*)'
        ]
        
        names_found = []
        for pattern in name_patterns:
            matches = re.findall(pattern, text, re.IGNORECASE)
            names_found.extend(matches)
        
        if names_found:
            # Unir los nombres encontrados
            full_name = ' '.join(names_found[:3])  # Máximo 3 componentes
            data['nombre_completo'] = full_name
        
        # Buscar fechas
        date_patterns = re.findall(r'\b\d{1,2}[/.-]\d{1,2}[/.-]\d{2,4}\b', text)
        if date_patterns:
            normalized_dates = []
            for date_str in date_patterns:
                normalized = normalize_date_safe(date_str)
                if normalized:
                    normalized_dates.append(normalized)
            
            if normalized_dates:
                normalized_dates.sort()
                # Asignar fechas según contexto
                if len(normalized_dates) >= 3:
                    data['fecha_nacimiento'] = normalized_dates[0]  # Más antigua
                    data['fecha_emision'] = normalized_dates[1]     # Media
                    data['fecha_expiracion'] = normalized_dates[-1] # Más reciente
                elif len(normalized_dates) == 2:
                    data['fecha_emision'] = normalized_dates[0]
                    data['fecha_expiracion'] = normalized_dates[1]
                else:
                    data['fecha_expiracion'] = normalized_dates[0]
        
        # Buscar género
        if re.search(r'\b(masculino|hombre|male|m)\b', text, re.IGNORECASE):
            data['genero'] = 'M'
        elif re.search(r'\b(femenino|mujer|female|f)\b', text, re.IGNORECASE):
            data['genero'] = 'F'
        
        # Buscar país de emisión para pasaportes
        if is_passport:
            country_patterns = [
                r'país[:\s]+([A-ZÁÉÍÓÚ][a-záéíóúñ]+)',
                r'country[:\s]+([A-Z][a-z]+)',
                r'nacionalidad[:\s]+([A-ZÁÉÍÓÚ][a-záéíóúñ]+)'
            ]
            for pattern in country_patterns:
                match = re.search(pattern, text, re.IGNORECASE)
                if match:
                    data['pais_emision'] = match.group(1)
                    data['nacionalidad'] = match.group(1)
                    break
        
        return data
        
    except Exception as e:
        logger.error(f"Error extrayendo datos de ID PyPDF2: {str(e)}")
        return {}

def extract_contract_data_pypdf2(text):
    """
    🆕 Extrae datos de contratos con PyPDF2
    """
    try:
        data = {
            'tipo_contrato': 'general',
            'numero_contrato': None,
            'fecha_contrato': None,
            'partes': [],
            'importe': None,
            'tasa_interes': None,
            'plazo': None,
            'entidad_bancaria': None
        }
        
        # Determinar tipo de contrato
        text_lower = text.lower()
        contract_types = {
            'cuenta': ['cuenta corriente', 'cuenta de ahorro', 'cuenta bancaria'],
            'tarjeta': ['tarjeta de crédito', 'tarjeta de débito', 'tarjeta bancaria'],
            'prestamo': ['préstamo', 'crédito', 'hipoteca', 'financiación'],
            'deposito': ['depósito', 'plazo fijo', 'inversión']
        }
        
        for tipo, keywords in contract_types.items():
            if any(keyword in text_lower for keyword in keywords):
                data['tipo_contrato'] = tipo
                break
        
        # Buscar número de contrato
        contract_patterns = [
            r'número.*?contrato[:\s]*([A-Z0-9/-]+)',
            r'contrato.*?número[:\s]*([A-Z0-9/-]+)',
            r'referencia[:\s]*([A-Z0-9/-]+)',
            r'nº.*?contrato[:\s]*([A-Z0-9/-]+)'
        ]
        
        for pattern in contract_patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                data['numero_contrato'] = match.group(1).strip()
                break
        
        # Buscar entidad bancaria
        bank_patterns = [
            r'(banco\s+[a-záéíóúñ]+(?:\s+[a-záéíóúñ]+)*)',
            r'(caja\s+[a-záéíóúñ]+(?:\s+[a-záéíóúñ]+)*)',
            r'(bbva|santander|caixabank|bankia|sabadell|unicaja)'
        ]
        
        for pattern in bank_patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                data['entidad_bancaria'] = match.group(1).strip()
                break
        
        # Buscar fechas
        date_matches = re.findall(r'\b\d{1,2}[/.-]\d{1,2}[/.-]\d{2,4}\b', text)
        if date_matches:
            normalized_date = normalize_date_safe(date_matches[0])
            if normalized_date:
                data['fecha_contrato'] = normalized_date
        
        # Buscar importes
        amount_patterns = [
            r'importe[:\s]*(\d{1,3}(?:[.,]\d{3})*(?:[.,]\d{2})?\s*€?)',
            r'cantidad[:\s]*(\d{1,3}(?:[.,]\d{3})*(?:[.,]\d{2})?\s*€?)',
            r'euros?[:\s]*(\d{1,3}(?:[.,]\d{3})*(?:[.,]\d{2})?)',
            r'(\d{1,3}(?:[.,]\d{3})*(?:[.,]\d{2})?\s*(?:€|euros?))'
        ]
        
        amounts_found = []
        for pattern in amount_patterns:
            matches = re.findall(pattern, text, re.IGNORECASE)
            amounts_found.extend(matches)
        
        if amounts_found:
            # Tomar el importe más grande como principal
            max_amount = max(amounts_found, key=lambda x: len(re.sub(r'[^\d]', '', x)))
            data['importe'] = max_amount
        
        # Buscar tasa de interés
        interest_patterns = [
            r'interés[:\s]*(\d{1,2}(?:[.,]\d{1,2})?\s*%)',
            r'tipo[:\s]*(\d{1,2}(?:[.,]\d{1,2})?\s*%)',
            r'(\d{1,2}(?:[.,]\d{1,2})?\s*%)\s*anual'
        ]
        
        for pattern in interest_patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                data['tasa_interes'] = match.group(1)
                break
        
        # Buscar plazo
        plazo_patterns = [
            r'plazo[:\s]*(\d+\s*(?:años?|meses?))',
            r'duración[:\s]*(\d+\s*(?:años?|meses?))',
            r'vencimiento[:\s]*(\d+\s*(?:años?|meses?))'
        ]
        
        for pattern in plazo_patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                data['plazo'] = match.group(1)
                break
        
        # Buscar partes del contrato
        party_patterns = [
            r'titular[:\s]+([A-ZÁÉÍÓÚÑ][a-záéíóúñ\s]+)',
            r'cliente[:\s]+([A-ZÁÉÍÓÚÑ][a-záéíóúñ\s]+)',
            r'contratante[:\s]+([A-ZÁÉÍÓÚÑ][a-záéíóúñ\s]+)'
        ]
        
        for pattern in party_patterns:
            matches = re.findall(pattern, text, re.IGNORECASE)
            data['partes'].extend(matches[:2])  # Máximo 2 partes
        
        return data
        
    except Exception as e:
        logger.error(f"Error extrayendo datos de contrato PyPDF2: {str(e)}")
        return {}

def extract_financial_data_pypdf2(text):
    """
    🆕 Extrae datos de documentos financieros con PyPDF2
    """
    try:
        data = {
            'tipo_documento': 'financiero',
            'periodo': None,
            'saldo_inicial': None,
            'saldo_final': None,
            'ingresos_total': None,
            'gastos_total': None,
            'numero_cuenta': None,
            'entidad': None
        }
        
        # Buscar número de cuenta
        account_patterns = [
            r'cuenta[:\s]*([A-Z]{2}\d{2}\s?\d{4}\s?\d{4}\s?\d{2}\s?\d{10})',  # IBAN
            r'cuenta[:\s]*(\d{4}\s?\d{4}\s?\d{2}\s?\d{10})',  # Cuenta nacional
            r'nº.*?cuenta[:\s]*(\d{10,20})'
        ]
        
        for pattern in account_patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                data['numero_cuenta'] = match.group(1).strip()
                break
        
        # Buscar saldos
        balance_patterns = [
            r'saldo.*?inicial[:\s]*(\d{1,3}(?:[.,]\d{3})*(?:[.,]\d{2})?\s*€?)',
            r'saldo.*?final[:\s]*(\d{1,3}(?:[.,]\d{3})*(?:[.,]\d{2})?\s*€?)',
            r'saldo.*?actual[:\s]*(\d{1,3}(?:[.,]\d{3})*(?:[.,]\d{2})?\s*€?)'
        ]
        
        balances = re.findall(r'saldo[:\s]*(\d{1,3}(?:[.,]\d{3})*(?:[.,]\d{2})?\s*€?)', text, re.IGNORECASE)
        if balances:
            if len(balances) >= 2:
                data['saldo_inicial'] = balances[0]
                data['saldo_final'] = balances[-1]
            else:
                data['saldo_final'] = balances[0]
        
        # Buscar período
        period_patterns = [
            r'desde[:\s]*(\d{1,2}[/.-]\d{1,2}[/.-]\d{2,4}).*?hasta[:\s]*(\d{1,2}[/.-]\d{1,2}[/.-]\d{2,4})',
            r'período[:\s]*(\d{1,2}[/.-]\d{1,2}[/.-]\d{2,4})',
            r'mes.*?(\d{1,2}[/.-]\d{2,4})'
        ]
        
        for pattern in period_patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                if len(match.groups()) > 1:
                    data['periodo'] = f"{match.group(1)} - {match.group(2)}"
                else:
                    data['periodo'] = match.group(1)
                break
        
        return data
        
    except Exception as e:
        logger.error(f"Error extrayendo datos financieros PyPDF2: {str(e)}")
        return {}

def extract_generic_data_pypdf2(text):
    """
    🆕 Extrae datos genéricos para documentos no clasificados
    """
    try:
        # Análisis básico del contenido
        word_count = len(text.split())
        line_count = text.count('\n') + 1
        
        # Detectar idioma aproximado
        spanish_indicators = ['que', 'con', 'por', 'para', 'desde', 'hasta', 'sobre']
        english_indicators = ['the', 'and', 'for', 'with', 'from', 'this', 'that']
        
        spanish_count = sum(1 for word in spanish_indicators if word in text.lower())
        english_count = sum(1 for word in english_indicators if word in text.lower())
        
        likely_language = 'spanish' if spanish_count > english_count else 'english'
        
        return {
            'tipo_contenido': 'documento_generico_pypdf2',
            'longitud_texto': len(text),
            'palabras_total': word_count,
            'lineas_total': line_count,
            'idioma_probable': likely_language,
            'texto_resumen': text[:500] if text else '',
            'contiene_numeros': bool(re.search(r'\d+', text)),
            'contiene_fechas': bool(re.search(r'\d{1,2}[/.-]\d{1,2}[/.-]\d{2,4}', text)),
            'contiene_emails': bool(re.search(r'\b[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}\b', text))
        }
        
    except Exception as e:
        logger.error(f"Error en extracción genérica PyPDF2: {str(e)}")
        return {}

def calculate_pypdf2_confidence(text, doc_type_info, structured_data):
    """
    🆕 Calcula confianza para extracción PyPDF2
    """
    try:
        confidence_factors = []
        
        # Factor 1: Confianza de clasificación del documento
        if doc_type_info and 'confidence' in doc_type_info:
            confidence_factors.append(doc_type_info['confidence'])
        
        # Factor 2: Longitud y calidad del texto
        text_length = len(text)
        if text_length > 3000:
            confidence_factors.append(0.85)
        elif text_length > 2000:
            confidence_factors.append(0.75)
        elif text_length > 1000:
            confidence_factors.append(0.65)
        elif text_length > 500:
            confidence_factors.append(0.55)
        else:
            confidence_factors.append(0.35)
        
        # Factor 3: Entidades estructuradas encontradas
        total_entities = sum(len(entities) for entities in structured_data.values())
        if total_entities > 8:
            confidence_factors.append(0.85)
        elif total_entities > 5:
            confidence_factors.append(0.75)
        elif total_entities > 3:
            confidence_factors.append(0.65)
        elif total_entities > 0:
            confidence_factors.append(0.55)
        else:
            confidence_factors.append(0.35)
        
        # Factor 4: Presencia de patrones específicos importantes
        important_patterns = ['dni', 'dates', 'amounts', 'contract_numbers']
        important_found = sum(1 for pattern in important_patterns if pattern in structured_data)
        
        if important_found >= 3:
            confidence_factors.append(0.80)
        elif important_found >= 2:
            confidence_factors.append(0.70)
        elif important_found >= 1:
            confidence_factors.append(0.60)
        else:
            confidence_factors.append(0.40)
        
        # Factor 5: Coherencia del texto (no fragmentado)
        line_count = text.count('\n')
        avg_line_length = len(text) / max(line_count, 1)
        
        if avg_line_length > 50:  # Líneas largas = mejor extracción
            confidence_factors.append(0.75)
        elif avg_line_length > 30:
            confidence_factors.append(0.65)
        else:
            confidence_factors.append(0.45)
        
        # Calcular promedio ponderado
        if confidence_factors:
            final_confidence = sum(confidence_factors) / len(confidence_factors)
        else:
            final_confidence = 0.5
        
        # Aplicar factor de reducción por ser PyPDF2 (menos sofisticado que Textract)
        final_confidence *= 0.90  # Reducir 10%
        
        # Limitar rango
        final_confidence = max(0.15, min(0.90, final_confidence))
        
        logger.debug(f"🔢 Confianza PyPDF2 calculada: {final_confidence:.3f} (factores: {len(confidence_factors)})")
        
        return final_confidence
        
    except Exception as e:
        logger.error(f"Error calculando confianza PyPDF2: {str(e)}")
        return 0.5

def normalize_date_safe(date_str):
    """
    🆕 Normalización segura de fechas (ya existía pero la mejoramos)
    """
    try:
        if not date_str or not isinstance(date_str, str):
            return None
        
        # Limpiar la cadena
        clean_date = re.sub(r'[^\d/.-]', '', date_str.strip())
        
        if re.match(r'\d{1,2}[/.-]\d{1,2}[/.-]\d{2,4}', clean_date):
            parts = re.split(r'[/.-]', clean_date)
            if len(parts) == 3:
                day, month, year = int(parts[0]), int(parts[1]), int(parts[2])
                
                # Normalizar año
                if year < 100:
                    year += 2000 if year < 50 else 1900
                
                # Validar rangos
                if 1 <= day <= 31 and 1 <= month <= 12 and 1900 <= year <= 2100:
                    return f"{year:04d}-{month:02d}-{day:02d}"
        
        return None
        
    except Exception:
        return None

def extract_text_from_pdf_with_textract(bucket, key):
    """
    Extrae texto de un PDF usando Textract con manejo de errores mejorado.
    Implementa fallbacks en caso de fallos y optimiza el procesamiento.
    """
    try:
        # Normalizar la clave
        key = unquote_plus(key)
        logger.info(f"Iniciando extracción de texto para {bucket}/{key}")
        
        document = {
            'S3Object': {
                'Bucket': bucket,
                'Name': key
            }
        }
        
        # Estrategia 1: Usar AnalyzeDocument (mejor para PDFs estructurados)
        try:
            logger.info(f"Método 1: AnalyzeDocument para {bucket}/{key}")
            response = operation_with_retry(
                textract_client.analyze_document,
                Document=document,
                FeatureTypes=['TABLES', 'FORMS']
            )
            
            # Extraer texto de manera eficiente
            text_blocks = [block.get('Text', '') for block in response.get('Blocks', []) 
                          if block.get('BlockType') == 'LINE']
            
            if text_blocks:
                full_text = ' '.join(text_blocks)
                logger.info(f"Extracción exitosa con AnalyzeDocument: {len(full_text)} caracteres")
                return full_text
        except Exception as e:
            logger.warning(f"AnalyzeDocument falló: {str(e)}")
        
        # Estrategia 2: Usar DetectDocumentText (mejor para PDFs simples o imágenes)
        try:
            logger.info(f"Método 2: DetectDocumentText para {bucket}/{key}")
            response = operation_with_retry(
                textract_client.detect_document_text,
                Document=document
            )
            
            text_blocks = [block.get('Text', '') for block in response.get('Blocks', []) 
                          if block.get('BlockType') == 'LINE']
            
            if text_blocks:
                full_text = ' '.join(text_blocks)
                logger.info(f"Extracción exitosa con DetectDocumentText: {len(full_text)} caracteres")
                return full_text
        except Exception as e:
            logger.warning(f"DetectDocumentText falló: {str(e)}")
        
        # Estrategia 3: Usar método alternativo cuando ambas APIs fallan
        logger.info(f"Métodos estándar fallaron, usando método alternativo para {bucket}/{key}")
        return extract_text_with_alternative_method(bucket, key)
        
    except Exception as e:
        logger.error(f"Error general en extract_text_from_pdf_with_textract: {str(e)}")
        return f"ERROR: No se pudo extraer texto. Detalle: {str(e)}"

def extract_text_with_alternative_method(bucket, key):
    """
    Método alternativo para extraer texto cuando las APIs estándar de Textract fallan.
    Ahora incluye PyPDF2 como opción adicional para extraer texto.
    """
    try:
        # Verificar si el documento existe
        try:
            operation_with_retry(
                s3_client.head_object,
                Bucket=bucket,
                Key=key
            )
        except Exception as e:
            logger.error(f"El documento {bucket}/{key} no existe: {str(e)}")
            return f"ERROR: Documento no encontrado en {bucket}/{key}"
        
        # Estrategia 1: Intentar con consultas específicas para extraer información
        try:
            logger.info(f"Intentando extracción con Queries para {bucket}/{key}")
            
            response = operation_with_retry(
                textract_client.analyze_document,
                Document={'S3Object': {'Bucket': bucket, 'Name': key}},
                FeatureTypes=['QUERIES'],
                QueriesConfig={
                    'Queries': [
                        {'Text': 'What is the document about?'},
                        {'Text': 'What is the main content?'},
                        {'Text': 'Who are the parties involved?'},
                        {'Text': 'What is the purpose of this document?'}
                    ]
                }
            )
            
            # Extraer respuestas a consultas
            query_results = []
            for block in response.get('Blocks', []):
                if block.get('BlockType') == 'QUERY_RESULT':
                    query_results.append(block.get('Text', ''))
            
            if query_results:
                return ' '.join(query_results)
            
        except Exception as query_error:
            logger.warning(f"Extracción con Queries falló: {str(query_error)}")
        
        # Estrategia 2: Intentar con PyPDF2 como último recurso
        logger.info("Intentando extracción con PyPDF2 como último recurso")
        pypdf_text = extract_text_with_pypdf2(bucket, key)
        
        if pypdf_text and not pypdf_text.startswith("ERROR:") and not pypdf_text.startswith("ADVERTENCIA:"):
            logger.info("Extracción exitosa usando PyPDF2")
            return pypdf_text
        else:
            logger.warning(f"PyPDF2 también falló: {pypdf_text}")
            
        # Mensaje final cuando todo falla
        logger.error(f"Todas las estrategias de extracción fallaron para {bucket}/{key}")
        return (f"ERROR: No se pudo extraer texto del documento {key}. " 
                "El formato puede no ser compatible con ninguno de los métodos disponibles.")
        
    except Exception as e:
        logger.error(f"Error crítico en método alternativo: {str(e)}")
        return f"ERROR: Fallo crítico en procesamiento: {str(e)}"

def process_pdf_for_textract(bucket, key, dest_bucket=None):
    """
    Procesa un PDF para mejorar compatibilidad con Textract.
    Implementa verificaciones eficientes y estrategias de procesamiento avanzadas.
    """
    if dest_bucket is None:
        dest_bucket = bucket
    
    try:
        # Extraer document_id del key para organización
        parts = key.split('/')
        document_id = parts[-2] if len(parts) > 1 else f"doc_{os.path.basename(key).split('.')[0]}"
        
        # Estrategia 1: Verificar compatibilidad directa con Textract
        try:
            logger.info(f"Verificando compatibilidad de {bucket}/{key} con Textract")
            
            # Realizar una prueba rápida con un fragmento de documento
            response = operation_with_retry(
                textract_client.analyze_document,
                Document={'S3Object': {'Bucket': bucket, 'Name': key}},
                FeatureTypes=['TABLES', 'FORMS']
            )
            
            # Si obtenemos respuesta, el documento es procesable directamente
            blocks_count = len(response.get('Blocks', []))
            logger.info(f"Documento compatible: se encontraron {blocks_count} bloques en {bucket}/{key}")
            
            return {
                "status": "success",
                "message": f"El PDF es compatible con Textract directamente ({blocks_count} bloques detectados)",
                "s3_key": key,
                "bucket": bucket,
                "direct_processing": True,
                "blocks_count": blocks_count
            }
            
        except Exception as analyze_error:
            # Documento no compatible directamente, registrar error específico
            error_message = str(analyze_error)
            error_type = type(analyze_error).__name__
            
            logger.warning(f"Documento no compatible con procesamiento directo: {error_type} - {error_message}")
            
            # Estrategia 2: Intentar con modo textual simple
            try:
                logger.info(f"Intentando modo de texto simple para {bucket}/{key}")
                
                response = operation_with_retry(
                    textract_client.detect_document_text,
                    Document={'S3Object': {'Bucket': bucket, 'Name': key}}
                )
                
                blocks_count = len(response.get('Blocks', []))
                if blocks_count > 0:
                    logger.info(f"Documento procesable en modo texto: {blocks_count} bloques")
                    return {
                        "status": "success",
                        "message": f"El PDF es procesable en modo texto simple ({blocks_count} bloques)",
                        "s3_key": key,
                        "bucket": bucket,
                        "direct_processing": True,
                        "text_only": True,
                        "blocks_count": blocks_count
                    }
            except Exception as text_error:
                logger.warning(f"Modo texto también falló: {str(text_error)}")
            
            # Estrategia 3: Verificar si podemos procesar con PyPDF2
            try:
                if PYPDF2_AVAILABLE:
                    logger.info(f"Intentando verificar compatibilidad con PyPDF2 para {bucket}/{key}")
                    # Hacemos una prueba rápida para ver si PyPDF2 puede leer el PDF
                    test_text = extract_text_with_pypdf2(bucket, key)
                    
                    if test_text and not test_text.startswith("ERROR:") and not test_text.startswith("ADVERTENCIA:"):
                        logger.info(f"Documento procesable con PyPDF2: {len(test_text)} caracteres extraídos")
                        return {
                            "status": "success",
                            "message": f"El PDF es procesable con PyPDF2 ({len(test_text)} caracteres)",
                            "s3_key": key,
                            "bucket": bucket,
                            "direct_processing": False,
                            "pypdf2_compatible": True
                        }
            except Exception as pypdf_error:
                logger.warning(f"Prueba de PyPDF2 falló: {str(pypdf_error)}")
            
            # Resultado cuando no se puede procesar directamente
            return {
                "status": "error",
                "message": f"El PDF no es compatible con Textract ni con PyPDF2: {error_type} - {error_message}",
                "s3_key": key,
                "bucket": bucket,
                "error_type": error_type,
                "error_details": error_message
            }
            
    except Exception as e:
        logger.error(f"Error general en process_pdf_for_textract: {str(e)}")
        return {
            "status": "error",
            "message": f"Error en verificación de compatibilidad: {str(e)}",
            "s3_key": key,
            "bucket": bucket
        }

def start_lending_analysis(bucket, key, document_id, textract_topic_arn=None, textract_role_arn=None, processed_bucket=None):
    """
    Inicia el análisis de documentos de préstamo/contratos usando Textract Analyze Lending.
    Implementa flujos asíncronos y síncronos con manejo completo de errores.
    """
    try:
        # Normalizar parámetros
        key = unquote_plus(key)
        if processed_bucket is None:
            processed_bucket = bucket
            
        logger.info(f"Iniciando análisis de documento para {document_id}: {bucket}/{key}")
        
        # Flujo 1: Análisis asíncrono si se proporcionan topic y rol
        if textract_topic_arn and textract_role_arn:
            try:
                # Configurar salida para resultados 
                output_config = {
                    'S3Bucket': processed_bucket,
                    'S3Prefix': f'lending_results/{document_id}'
                }
                
                # Iniciar análisis asíncrono con manejo de errores
                response = operation_with_retry(
                    textract_client.start_lending_analysis,
                    DocumentLocation={'S3Object': {'Bucket': bucket, 'Name': key}},
                    OutputConfig=output_config,
                    JobTag=f"{document_id}_lending_analysis",
                    NotificationChannel={
                        'SNSTopicArn': textract_topic_arn,
                        'RoleArn': textract_role_arn
                    }
                )
                
                job_id = response['JobId']
                logger.info(f"Análisis asíncrono iniciado correctamente: JobId={job_id}")
                return {
                    "status": "success",
                    "job_id": job_id,
                    "message": "Análisis asíncrono iniciado correctamente",
                    "output_location": f"s3://{processed_bucket}/lending_results/{document_id}/"
                }
            except Exception as async_error:
                logger.warning(f"Error en inicio asíncrono: {str(async_error)}")
                # Continuamos con estrategia síncrona
        
        # Flujo 2: Análisis síncrono como fallback o si no hay configuración asíncrona
        try:
            logger.info(f"Intentando análisis síncrono para {document_id}")
            
            # Utilizamos analyze_document con queries avanzadas
            response = operation_with_retry(
                textract_client.analyze_document,
                Document={'S3Object': {'Bucket': bucket, 'Name': key}},
                FeatureTypes=['TABLES', 'FORMS', 'QUERIES'],
                QueriesConfig={
                    'Queries': [
                        {'Text': 'What is the contract number?'},
                        {'Text': 'What is the start date?'},
                        {'Text': 'What is the end date?'},
                        {'Text': 'What is the interest rate?'},
                        {'Text': 'Who are the signatories?'},
                        {'Text': 'What is the loan amount?'},
                        {'Text': 'What is the contract type?'},
                        {'Text': 'What are the main terms and conditions?'}
                    ]
                }
            )
            
            # Extraer información de queries para resultados rápidos
            query_results = {}
            for block in response.get('Blocks', []):
                if block.get('BlockType') == 'QUERY' and 'Relationships' in block:
                    query_text = block.get('Query', {}).get('Text', '')
                    for rel in block.get('Relationships', []):
                        if rel.get('Type') == 'ANSWER':
                            # Buscar bloques de respuesta
                            answer_blocks = [b for b in response.get('Blocks', []) 
                                            if b.get('Id') in rel.get('Ids', []) and b.get('BlockType') == 'QUERY_RESULT']
                            
                            # Extraer texto de respuesta
                            answers = [answer.get('Text', '') for answer in answer_blocks]
                            if answers:
                                query_results[query_text] = ' '.join(answers)
            
            logger.info(f"Análisis síncrono completado con {len(query_results)} respuestas a consultas")
            return {
                "status": "success",
                "is_sync": True,
                "message": "Análisis síncrono completado correctamente",
                "query_results": query_results,
                "block_count": len(response.get('Blocks', [])),
                "document_id": document_id
            }
            
        except Exception as sync_error:
            error_type = type(sync_error).__name__
            error_message = str(sync_error)
            logger.error(f"Error en análisis síncrono: {error_type} - {error_message}")
            
            # Verificar si podemos usar PyPDF2 como último recurso
            if PYPDF2_AVAILABLE and "UnsupportedDocumentException" in error_message:
                logger.info(f"Intentando extracción con PyPDF2 como alternativa a Textract para {document_id}")
                text = extract_text_with_pypdf2(bucket, key)
                
                if text and not text.startswith("ERROR:") and not text.startswith("ADVERTENCIA:"):
                    logger.info(f"Extracción con PyPDF2 exitosa para {document_id}: {len(text)} caracteres")
                    # Creamos una estructura similar a la respuesta de Textract para consistencia
                    return {
                        "status": "success",
                        "is_sync": True,
                        "message": "Análisis completado con PyPDF2 (alternativa a Textract)",
                        "query_results": {
                            "What is the document about?": "Contrato o documento textual extraído con PyPDF2",
                            "What is the main content?": text[:500] + "..." if len(text) > 500 else text
                        },
                        "extracted_text": text,
                        "method": "pypdf2_fallback",
                        "document_id": document_id
                    }
            
            # Devolver información detallada sobre el error
            return {
                "status": "error",
                "message": f"Error en análisis de documento: {error_message}",
                "error_type": error_type,
                "document_id": document_id,
                "s3_key": key
            }
    except Exception as e:
        logger.error(f"Error crítico en start_lending_analysis: {str(e)}")
        return {
            "status": "error",
            "message": f"Error crítico: {str(e)}",
            "document_id": document_id
        }