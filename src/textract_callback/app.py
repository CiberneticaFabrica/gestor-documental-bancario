# FIXED: src/textract_callback/app.py - Complete fixed version
import os
import json
import boto3
import logging
import sys
import re
import time
from datetime import datetime

# Agregar las rutas para importar módulos comunes
sys.path.append('/opt/python')
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Configurar el logger
logger = logging.getLogger()
logger.setLevel(os.environ.get('LOG_LEVEL', 'INFO'))

# Instanciar clientes de AWS con configuración de reintentos
from botocore.config import Config
retry_config = Config(
    retries={
        'max_attempts': 3,
        'mode': 'standard'
    }
)

# Inicializar clientes AWS
textract_client = boto3.client('textract', config=retry_config)
s3_client = boto3.client('s3', config=retry_config)
sqs_client = boto3.client('sqs', config=retry_config)

# Obtener variables de entorno
DOCUMENT_CLASSIFIER_QUEUE_URL = os.environ.get('DOCUMENT_CLASSIFIER_QUEUE_URL')
CONTRACT_PROCESSOR_QUEUE_URL = os.environ.get('CONTRACT_PROCESSOR_QUEUE_URL')
ID_PROCESSOR_QUEUE_URL = os.environ.get('ID_PROCESSOR_QUEUE_URL')
FINANCIAL_PROCESSOR_QUEUE_URL = os.environ.get('FINANCIAL_PROCESSOR_QUEUE_URL')
DEFAULT_PROCESSOR_QUEUE_URL = os.environ.get('DEFAULT_PROCESSOR_QUEUE_URL', DOCUMENT_CLASSIFIER_QUEUE_URL)

# Importar funciones necesarias
try:
    from common.db_connector import (
        update_document_processing_status, 
        update_document_extraction_data,
        update_analysis_record,
        get_document_type_by_id,
        get_document_by_id,
        get_version_id,
        execute_query,
        generate_uuid,
        insert_analysis_record,
        log_document_processing_start,
        log_document_processing_end 
    )
    from common.validation import guess_document_type
    logger.info("Módulos importados correctamente")
except ImportError as e:
    logger.error(f"Error importando módulos: {str(e)}")

def extract_complete_data_from_textract_fixed(textract_results):
    """
    VERSIÓN CORREGIDA: Extrae TODOS los datos de Textract de manera robusta
    """
    start_time = time.time()
    logger.info(f"🔄 Iniciando extracción completa de datos de Textract (versión corregida)")
    
    try:
        # 1. Validación de entrada
        if not textract_results or not isinstance(textract_results, list):
            logger.error("❌ Resultados de Textract inválidos o vacíos")
            return create_empty_extraction_result()
        
        # 2. Obtener todos los bloques de todas las páginas
        all_blocks = []
        page_count = 0
        
        for page_index, page in enumerate(textract_results):
            if not isinstance(page, dict):
                logger.warning(f"⚠️ Página {page_index} no es un diccionario válido")
                continue
                
            page_blocks = page.get('Blocks', [])
            if page_blocks:
                all_blocks.extend(page_blocks)
                page_count += 1
                logger.debug(f"📄 Página {page_index}: {len(page_blocks)} bloques")
        
        logger.info(f"📊 Total procesado: {len(all_blocks)} bloques de {page_count} páginas")
        
        if not all_blocks:
            logger.warning("⚠️ No se encontraron bloques en los resultados")
            return create_empty_extraction_result()
        
        # 3. Metadata del documento
        doc_metadata = {
            'blocks_count': len(all_blocks),
            'page_count': page_count,
            'detection_time': textract_results[0].get('DetectDocumentTextModelVersion', 
                                                     textract_results[0].get('DocumentMetadata', {}).get('Version', 'unknown')),
            'extraction_timestamp': datetime.now().isoformat()
        }
        
        # 4. Extracción de texto completo
        logger.debug("📝 Extrayendo texto completo...")
        text_data = extract_text_from_blocks(all_blocks)
        
        # 5. Extracción de tablas
        logger.debug("📋 Extrayendo tablas...")
        tables_data = extract_tables_from_blocks(all_blocks)
        
        # 6. Extracción de formularios (key-value pairs)
        logger.debug("📝 Extrayendo formularios...")
        forms_data = extract_forms_from_blocks(all_blocks)
        
        # 7. Extracción de queries (si existen)
        logger.debug("❓ Extrayendo queries...")
        query_data = extract_query_answers_from_blocks(all_blocks)
        
        # 8. Extracción de datos estructurados mediante patrones
        logger.debug("🔍 Extrayendo datos estructurados...")
        structured_data = extract_structured_patterns(text_data['full_text'])
        
        # 9. Clasificación del documento
        logger.debug("🏷️ Clasificando documento...")
        doc_type_info = guess_document_type(text_data['full_text'])
        
        # 10. Extracción de datos específicos según tipo
        logger.debug("🎯 Extrayendo datos específicos...")
        specific_data = extract_type_specific_data_safe(
            text_data['full_text'], 
            forms_data, 
            structured_data, 
            doc_type_info['document_type']
        )
        
        # 11. Procesamiento de queries estructuradas
        structured_query_data = process_query_answers_by_type_safe(query_data)
        
        # 12. Cálculo de confianza
        extraction_confidence = calculate_overall_confidence(query_data, doc_type_info, text_data)
        
        # 13. Resultado final
        processing_time = time.time() - start_time
        
        result = {
            'metadata': doc_metadata,
            'full_text': text_data['full_text'],
            'text_blocks': text_data['blocks'],
            'tables': tables_data,
            'forms': forms_data,
            'structured_data': structured_data,
            'specific_data': specific_data,
            'doc_type_info': doc_type_info,
            'query_answers': query_data,
            'structured_query_data': structured_query_data,
            'extraction_confidence': extraction_confidence,
            'processing_time': processing_time,
            'extraction_success': True
        }
        
        logger.info(f"✅ Extracción completa exitosa:")
        logger.info(f"   - Tiempo: {processing_time:.2f}s")
        logger.info(f"   - Texto: {len(text_data['full_text']):,} caracteres")
        logger.info(f"   - Tablas: {len(tables_data)}")
        logger.info(f"   - Formularios: {len(forms_data)} campos")
        logger.info(f"   - Queries: {len(query_data)} respuestas")
        logger.info(f"   - Confianza: {extraction_confidence:.2f}")
        
        return result
        
    except Exception as e:
        logger.error(f"❌ Error en extracción completa: {str(e)}")
        import traceback
        logger.error(f"📍 Stack trace: {traceback.format_exc()}")
        
        # Retornar resultado de error pero funcional
        return create_error_extraction_result(str(e), time.time() - start_time)

def extract_text_from_blocks(all_blocks):
    """Extrae texto de los bloques de manera robusta"""
    text_blocks = []
    
    try:
        for block in all_blocks:
            if block.get('BlockType') == 'LINE' and 'Text' in block:
                text_blocks.append({
                    'text': block.get('Text', ''),
                    'page': block.get('Page', 1),
                    'confidence': block.get('Confidence', 0),
                    'id': block.get('Id', '')
                })
        
        # Ordenar por página
        text_blocks.sort(key=lambda x: x['page'])
        full_text = ' '.join([block['text'] for block in text_blocks if block['text'].strip()])
        
        return {
            'full_text': full_text,
            'blocks': text_blocks,
            'character_count': len(full_text),
            'line_count': len(text_blocks)
        }
        
    except Exception as e:
        logger.error(f"Error extrayendo texto: {str(e)}")
        return {
            'full_text': '',
            'blocks': [],
            'character_count': 0,
            'line_count': 0
        }

def extract_tables_from_blocks(all_blocks):
    """Extrae tablas de los bloques de manera robusta"""
    tables = []
    
    try:
        table_blocks = [block for block in all_blocks if block.get('BlockType') == 'TABLE']
        
        for table_block in table_blocks:
            table_id = table_block.get('Id')
            page_num = table_block.get('Page', 1)
            
            # Encontrar celdas de la tabla
            cell_blocks = [
                block for block in all_blocks 
                if block.get('BlockType') == 'CELL' and block.get('Page') == page_num
            ]
            
            if cell_blocks:
                table_data = process_table_cells(cell_blocks, all_blocks)
                
                if table_data:
                    tables.append({
                        'id': table_id,
                        'page': page_num,
                        'rows': len(table_data),
                        'columns': len(table_data[0]) if table_data else 0,
                        'data': table_data
                    })
        
        logger.debug(f"📋 {len(tables)} tablas extraídas")
        return tables
        
    except Exception as e:
        logger.error(f"Error extrayendo tablas: {str(e)}")
        return []

def process_table_cells(cell_blocks, all_blocks):
    """Procesa las celdas de una tabla"""
    try:
        if not cell_blocks:
            return []
        
        # Determinar dimensiones de la tabla
        max_row = max([cell.get('RowIndex', 0) for cell in cell_blocks])
        max_col = max([cell.get('ColumnIndex', 0) for cell in cell_blocks])
        
        if max_row == 0 or max_col == 0:
            return []
        
        # Crear matriz vacía
        table_data = []
        for _ in range(max_row):
            table_data.append([''] * max_col)
        
        # Rellenar celdas
        for cell in cell_blocks:
            row_idx = cell.get('RowIndex', 1) - 1
            col_idx = cell.get('ColumnIndex', 1) - 1
            
            if 0 <= row_idx < len(table_data) and 0 <= col_idx < len(table_data[row_idx]):
                cell_text = extract_cell_text(cell, all_blocks)
                table_data[row_idx][col_idx] = cell_text
        
        return table_data
        
    except Exception as e:
        logger.error(f"Error procesando celdas: {str(e)}")
        return []

def extract_cell_text(cell, all_blocks):
    """Extrae texto de una celda específica"""
    try:
        cell_text = ''
        
        # Buscar texto en relaciones CHILD
        for rel in cell.get('Relationships', []):
            if rel.get('Type') == 'CHILD':
                child_ids = rel.get('Ids', [])
                
                word_blocks = [
                    block for block in all_blocks
                    if block.get('Id') in child_ids and block.get('BlockType') == 'WORD'
                ]
                
                if word_blocks:
                    cell_text = ' '.join([word.get('Text', '') for word in word_blocks])
                    break
        
        # Fallback: usar texto directo si existe
        if not cell_text and 'Text' in cell:
            cell_text = cell.get('Text', '')
        
        return cell_text.strip()
        
    except Exception as e:
        logger.error(f"Error extrayendo texto de celda: {str(e)}")
        return ''

def extract_forms_from_blocks(all_blocks):
    """Extrae formularios (key-value pairs) de manera robusta"""
    forms = {}
    
    try:
        key_blocks = [
            block for block in all_blocks
            if (block.get('BlockType') == 'KEY_VALUE_SET' and 
                'EntityTypes' in block and 'KEY' in block.get('EntityTypes', []))
        ]
        
        for key_block in key_blocks:
            key_text = extract_key_text(key_block, all_blocks)
            value_text = extract_value_text(key_block, all_blocks)
            
            if key_text and key_text.strip():
                forms[key_text.strip()] = value_text.strip()
        
        logger.debug(f"📝 {len(forms)} campos de formulario extraídos")
        return forms
        
    except Exception as e:
        logger.error(f"Error extrayendo formularios: {str(e)}")
        return {}

def extract_key_text(key_block, all_blocks):
    """Extrae texto de una clave"""
    try:
        for rel in key_block.get('Relationships', []):
            if rel.get('Type') == 'CHILD':
                child_ids = rel.get('Ids', [])
                
                key_words = [
                    block.get('Text', '') for block in all_blocks
                    if block.get('Id') in child_ids and block.get('BlockType') == 'WORD'
                ]
                
                if key_words:
                    return ' '.join(key_words)
        
        return ''
        
    except Exception as e:
        logger.error(f"Error extrayendo texto de clave: {str(e)}")
        return ''

def extract_value_text(key_block, all_blocks):
    """Extrae texto del valor asociado a una clave"""
    try:
        # Buscar relaciones VALUE
        for rel in key_block.get('Relationships', []):
            if rel.get('Type') == 'VALUE':
                value_ids = rel.get('Ids', [])
                
                for value_id in value_ids:
                    value_block = next((b for b in all_blocks if b.get('Id') == value_id), None)
                    
                    if value_block:
                        # Buscar palabras del valor
                        for value_rel in value_block.get('Relationships', []):
                            if value_rel.get('Type') == 'CHILD':
                                value_word_ids = value_rel.get('Ids', [])
                                
                                value_words = [
                                    block.get('Text', '') for block in all_blocks
                                    if block.get('Id') in value_word_ids and block.get('BlockType') == 'WORD'
                                ]
                                
                                if value_words:
                                    return ' '.join(value_words)
        
        return ''
        
    except Exception as e:
        logger.error(f"Error extrayendo texto de valor: {str(e)}")
        return ''

def extract_query_answers_from_blocks(all_blocks):
    """Extrae respuestas de queries de manera robusta"""
    query_answers = {}
    
    try:
        query_blocks = [block for block in all_blocks if block.get('BlockType') == 'QUERY']
        
        for block in query_blocks:
            query_id = block.get('Id')
            query_text = block.get('Query', {}).get('Text', '')
            query_alias = block.get('Query', {}).get('Alias', '')
            
            # Buscar respuesta
            for rel in block.get('Relationships', []):
                if rel.get('Type') == 'ANSWER':
                    for answer_id in rel.get('Ids', []):
                        answer_block = next(
                            (b for b in all_blocks 
                             if b.get('Id') == answer_id and b.get('BlockType') == 'QUERY_RESULT'), 
                            None
                        )
                        
                        if answer_block:
                            answer_text = answer_block.get('Text', '')
                            confidence = answer_block.get('Confidence', 0)
                            
                            key = query_alias or query_text
                            query_answers[key] = {
                                'question': query_text,
                                'answer': answer_text,
                                'confidence': confidence,
                                'query_id': query_id
                            }
                            break
        
        logger.debug(f"❓ {len(query_answers)} queries respondidas")
        return query_answers
        
    except Exception as e:
        logger.error(f"Error extrayendo queries: {str(e)}")
        return {}

def extract_structured_patterns(text):
    """Extrae patrones estructurados del texto"""
    try:
        patterns = {
            'dni': r'\b\d{8}[A-Za-z]?\b',
            'passport': r'\b[A-Z]{1,2}[0-9]{6,7}\b',
            'email': r'\b[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}\b',
            'phone': r'\b(?:\+\d{1,3}\s?)?\(?\d{1,4}\)?[\s.-]?\d{3}[\s.-]?\d{4}\b',
            'iban': r'\b[A-Z]{2}\d{2}[A-Z0-9]{4}\d{7}([A-Z0-9]?){0,16}\b',
            'dates': r'\b\d{1,2}[/.-]\d{1,2}[/.-]\d{2,4}\b',
            'amounts': r'\b\d{1,3}(?:[.,]\d{3})*(?:[.,]\d{2})?\s*(?:€|EUR|USD|\$)?\b',
            'percentages': r'\b\d{1,2}(?:[.,]\d{1,2})?\s*%\b'
        }
        
        structured_data = {}
        for entity_type, pattern in patterns.items():
            matches = re.findall(pattern, text)
            if matches:
                # Limpiar duplicados manteniendo orden
                unique_matches = []
                seen = set()
                for match in matches:
                    if match not in seen:
                        unique_matches.append(match)
                        seen.add(match)
                structured_data[entity_type] = unique_matches
        
        logger.debug(f"🔍 Patrones extraídos: {list(structured_data.keys())}")
        return structured_data
        
    except Exception as e:
        logger.error(f"Error extrayendo patrones: {str(e)}")
        return {}

def extract_type_specific_data_safe(text, forms, structured_data, doc_type):
    """Versión segura de extracción de datos específicos"""
    try:
        if doc_type in ['dni', 'pasaporte', 'cedula']:
            return extract_id_document_data_safe(text, forms, structured_data, doc_type == 'pasaporte')
        elif doc_type in ['contrato', 'contrato_cuenta', 'contrato_tarjeta']:
            return extract_contract_data_safe(text, forms, structured_data)
        else:
            return extract_generic_data_safe(text, forms, structured_data)
            
    except Exception as e:
        logger.error(f"Error en extracción específica: {str(e)}")
        return {}

def extract_id_document_data_safe(text, forms, structured_data, is_passport=False):
    """Extracción segura de datos de documentos de identidad"""
    try:
        data = {
            'tipo_identificacion': 'pasaporte' if is_passport else 'dni',
            'numero_identificacion': None,
            'nombre_completo': None,
            'fecha_nacimiento': None,
            'fecha_emision': None,
            'fecha_expiracion': None,
            'pais_emision': 'España',
            'genero': None
        }
        
        # Buscar en formularios primero (más confiable)
        form_mappings = {
            'nombre': ['nombre', 'apellidos', 'name', 'surname'],
            'numero': ['documento', 'número', 'dni', 'passport'],
            'emision': ['expedición', 'emisión', 'issue'],
            'caducidad': ['caducidad', 'validez', 'expiry'],
            'nacimiento': ['nacimiento', 'birth']
        }
        
        for field, keywords in form_mappings.items():
            for keyword in keywords:
                for form_key in forms:
                    if keyword.lower() in form_key.lower():
                        value = forms[form_key]
                        
                        if field == 'nombre' and not data['nombre_completo']:
                            data['nombre_completo'] = value
                        elif field == 'numero' and not data['numero_identificacion']:
                            data['numero_identificacion'] = value
                        elif field == 'emision' and not data['fecha_emision']:
                            data['fecha_emision'] = normalize_date_safe(value)
                        elif field == 'caducidad' and not data['fecha_expiracion']:
                            data['fecha_expiracion'] = normalize_date_safe(value)
                        elif field == 'nacimiento' and not data['fecha_nacimiento']:
                            data['fecha_nacimiento'] = normalize_date_safe(value)
        
        # Fallback a datos estructurados
        if not data['numero_identificacion']:
            if is_passport and 'passport' in structured_data:
                data['numero_identificacion'] = structured_data['passport'][0]
            elif not is_passport and 'dni' in structured_data:
                data['numero_identificacion'] = structured_data['dni'][0]
        
        # Fallback para fechas
        if 'dates' in structured_data and len(structured_data['dates']) >= 2:
            dates = [normalize_date_safe(d) for d in structured_data['dates']]
            valid_dates = [d for d in dates if d]
            
            if valid_dates:
                valid_dates.sort()
                if not data['fecha_emision'] and len(valid_dates) > 0:
                    data['fecha_emision'] = valid_dates[0]
                if not data['fecha_expiracion'] and len(valid_dates) > 1:
                    data['fecha_expiracion'] = valid_dates[-1]
        
        return data
        
    except Exception as e:
        logger.error(f"Error en extracción de ID: {str(e)}")
        return {}

def extract_contract_data_safe(text, forms, structured_data):
    """Extracción segura de datos de contratos"""
    try:
        data = {
            'tipo_contrato': 'general',
            'numero_contrato': None,
            'fecha_contrato': None,
            'partes': [],
            'importe': None,
            'tasa_interes': None
        }
        
        # Determinar tipo de contrato
        contract_keywords = {
            'cuenta': ['cuenta corriente', 'cuenta de ahorro'],
            'tarjeta': ['tarjeta de crédito', 'tarjeta de débito'],
            'prestamo': ['préstamo', 'crédito', 'hipoteca']
        }
        
        text_lower = text.lower()
        for tipo, keywords in contract_keywords.items():
            if any(keyword in text_lower for keyword in keywords):
                data['tipo_contrato'] = tipo
                break
        
        # Buscar número de contrato
        contract_patterns = [
            r'[Nn]úmero de [Cc]ontrato:?\s*([A-Z0-9-]+)',
            r'[Cc]ontrato [Nn]úmero:?\s*([A-Z0-9-]+)',
            r'[Rr]eferencia:?\s*([A-Z0-9-]+)'
        ]
        
        for pattern in contract_patterns:
            match = re.search(pattern, text)
            if match:
                data['numero_contrato'] = match.group(1).strip()
                break
        
        # Buscar fechas
        if 'dates' in structured_data and structured_data['dates']:
            data['fecha_contrato'] = normalize_date_safe(structured_data['dates'][0])
        
        # Buscar importes
        if 'amounts' in structured_data and structured_data['amounts']:
            amounts = []
            for amount_str in structured_data['amounts']:
                try:
                    clean_amount = re.sub(r'[€$,.]', '', amount_str).strip()
                    if clean_amount.isdigit():
                        amounts.append((int(clean_amount), amount_str))
                except:
                    continue
            
            if amounts:
                amounts.sort(key=lambda x: x[0], reverse=True)
                data['importe'] = amounts[0][1]
        
        # Buscar tasa de interés
        if 'percentages' in structured_data and structured_data['percentages']:
            data['tasa_interes'] = structured_data['percentages'][0]
        
        return data
        
    except Exception as e:
        logger.error(f"Error en extracción de contrato: {str(e)}")
        return {}

def extract_generic_data_safe(text, forms, structured_data):
    """Extracción segura de datos genéricos"""
    try:
        return {
            'texto_resumen': text[:500] if text else '',
            'campos_formulario': len(forms),
            'entidades_encontradas': list(structured_data.keys()),
            'tipo_contenido': 'documento_generico'
        }
    except Exception as e:
        logger.error(f"Error en extracción genérica: {str(e)}")
        return {}

def normalize_date_safe(date_str):
    """Normalización segura de fechas"""
    try:
        if not date_str or not isinstance(date_str, str):
            return None
        
        clean_date = re.sub(r'[^\d/.-]', '', date_str)
        
        if re.match(r'\d{1,2}[/.-]\d{1,2}[/.-]\d{2,4}', clean_date):
            parts = re.split(r'[/.-]', clean_date)
            if len(parts) == 3:
                day, month, year = int(parts[0]), int(parts[1]), int(parts[2])
                
                if year < 100:
                    year += 2000 if year < 50 else 1900
                
                if 1 <= day <= 31 and 1 <= month <= 12 and 1900 <= year <= 2100:
                    return f"{year:04d}-{month:02d}-{day:02d}"
        
        return None
        
    except Exception:
        return None

def process_query_answers_by_type_safe(query_answers):
    """Procesamiento seguro de respuestas de queries"""
    try:
        structured_data = {
            'contract_info': {},
            'parties': {},
            'financial_terms': {},
            'guarantees': {},
            'personal_info': {}
        }
        
        field_mapping = {
            'numero_contrato': ('contract_info', 'contract_number'),
            'nombre_prestatario': ('parties', 'borrower_name'),
            'cedula_prestatario': ('parties', 'borrower_id'),
            'monto_prestamo': ('financial_terms', 'loan_amount'),
            'tasa_interes': ('financial_terms', 'interest_rate'),
            'cuota_mensual': ('financial_terms', 'monthly_payment'),
            'nombre_completo': ('personal_info', 'full_name'),
            'numero_documento': ('personal_info', 'document_number'),
            'fecha_contrato': ('contract_info', 'contract_date'),
            'nombre_banco': ('parties', 'bank_name')
        }
        
        for alias, answer_data in query_answers.items():
            if alias in field_mapping:
                category, field = field_mapping[alias]
                
                clean_answer = clean_query_answer_safe(answer_data['answer'])
                if clean_answer:
                    structured_data[category][field] = {
                        'value': clean_answer,
                        'confidence': answer_data['confidence'],
                        'raw_answer': answer_data['answer']
                    }
        
        return structured_data
        
    except Exception as e:
        logger.error(f"Error procesando queries: {str(e)}")
        return {}

def clean_query_answer_safe(raw_answer):
    """Limpieza segura de respuestas de queries"""
    try:
        if not raw_answer or not isinstance(raw_answer, str):
            return None
        
        # Limpiar respuestas "no encontrado"
        no_answer_patterns = ['not found', 'no encontrado', 'n/a', 'none', '']
        cleaned = raw_answer.strip().lower()
        
        if cleaned in no_answer_patterns or len(cleaned) < 2:
            return None
        
        return raw_answer.strip()
        
    except Exception:
        return None

def calculate_overall_confidence(query_data, doc_type_info, text_data):
    """Calcula confianza general de la extracción"""
    try:
        confidences = []
        
        # Confianza de clasificación
        if doc_type_info and 'confidence' in doc_type_info:
            confidences.append(doc_type_info['confidence'])
        
        # Confianza de queries
        if query_data:
            query_confidences = [
                data['confidence'] for data in query_data.values() 
                if data.get('answer') and data['answer'].strip()
            ]
            if query_confidences:
                confidences.append(sum(query_confidences) / len(query_confidences) / 100)
        
        # Confianza basada en cantidad de texto
        text_length = len(text_data.get('full_text', ''))
        if text_length > 1000:
            confidences.append(0.9)
        elif text_length > 500:
            confidences.append(0.7)
        elif text_length > 100:
            confidences.append(0.5)
        else:
            confidences.append(0.3)
        
        if confidences:
            return sum(confidences) / len(confidences)
        else:
            return 0.5
            
    except Exception as e:
        logger.error(f"Error calculando confianza: {str(e)}")
        return 0.5

def create_empty_extraction_result():
    """Crea un resultado vacío pero válido"""
    return {
        'metadata': {'blocks_count': 0, 'page_count': 0, 'detection_time': 'unknown'},
        'full_text': '',
        'text_blocks': [],
        'tables': [],
        'forms': {},
        'structured_data': {},
        'specific_data': {},
        'doc_type_info': {'document_type': 'unknown', 'confidence': 0.0},
        'query_answers': {},
        'structured_query_data': {},
        'extraction_confidence': 0.0,
        'processing_time': 0.0,
        'extraction_success': False
    }

def create_error_extraction_result(error_message, processing_time):
    """Crea un resultado de error pero funcional"""
    result = create_empty_extraction_result()
    result.update({
        'error_message': error_message,
        'processing_time': processing_time,
        'extraction_success': False
    })
    return result

def determine_processor_queue(doc_type):
    """Determina la cola SQS apropiada según el tipo de documento"""
    queue_mapping = {
        'dni': ID_PROCESSOR_QUEUE_URL,
        'pasaporte': ID_PROCESSOR_QUEUE_URL,
        'cedula': ID_PROCESSOR_QUEUE_URL,
        'contrato': CONTRACT_PROCESSOR_QUEUE_URL,
        'contrato_cuenta': CONTRACT_PROCESSOR_QUEUE_URL,
        'contrato_tarjeta': CONTRACT_PROCESSOR_QUEUE_URL,
        'extracto': FINANCIAL_PROCESSOR_QUEUE_URL,
        'extracto_bancario': FINANCIAL_PROCESSOR_QUEUE_URL,
        'nomina': FINANCIAL_PROCESSOR_QUEUE_URL,
        'impuesto': FINANCIAL_PROCESSOR_QUEUE_URL
    }
    
    for key, queue in queue_mapping.items():
        if doc_type.lower() == key or doc_type.lower().startswith(key):
            return queue or DEFAULT_PROCESSOR_QUEUE_URL
    
    return DEFAULT_PROCESSOR_QUEUE_URL

def get_document_id_from_job_tag(job_tag):
    """Extrae el ID del documento desde el job tag"""
    try:
        if not job_tag:
            return None
        
        parts = job_tag.split('_')
        if len(parts) > 0:
            return parts[0]
        
        return job_tag
        
    except Exception as e:
        logger.error(f"Error extrayendo document_id: {str(e)}")
        return None

def get_textract_results_safe(job_id, job_type='DOCUMENT_ANALYSIS'):
    """Obtiene resultados de Textract de manera segura"""
    pages = []
    start_time = time.time()
    
    try:
        logger.info(f"🔄 Obteniendo resultados de Textract: JobId={job_id}, Tipo={job_type}")
        
        max_pages = 50  # Límite de seguridad
        page_count = 0
        
        if job_type == 'DOCUMENT_ANALYSIS':
            response = textract_client.get_document_analysis(JobId=job_id)
            pages.append(response)
            page_count += 1
            
            next_token = response.get('NextToken')
            while next_token and page_count < max_pages:
                response = textract_client.get_document_analysis(
                    JobId=job_id,
                    NextToken=next_token
                )
                pages.append(response)
                page_count += 1
                next_token = response.get('NextToken')
                
        elif job_type == 'DOCUMENT_TEXT_DETECTION':
            response = textract_client.get_document_text_detection(JobId=job_id)
            pages.append(response)
            page_count += 1
            
            next_token = response.get('NextToken')
            while next_token and page_count < max_pages:
                response = textract_client.get_document_text_detection(
                    JobId=job_id,
                    NextToken=next_token
                )
                pages.append(response)
                page_count += 1
                next_token = response.get('NextToken')
        
        processing_time = time.time() - start_time
        logger.info(f"✅ Resultados obtenidos: {page_count} páginas en {processing_time:.2f}s")
        
        return pages, processing_time
        
    except Exception as e:
        logger.error(f"❌ Error obteniendo resultados: {str(e)}")
        return [], time.time() - start_time

def validate_document_version_consistency(document_id):
    """
    Valida la consistencia entre documento y versiones
    """
    try:
        # Obtener información del documento
        doc_query = """
        SELECT id_documento, version_actual 
        FROM documentos 
        WHERE id_documento = %s
        """
        doc_result = execute_query(doc_query, (document_id,))
        
        if not doc_result:
            return False, "Documento no encontrado"
        
        version_actual = doc_result[0]['version_actual']
        
        # Verificar que la versión actual existe
        version_query = """
        SELECT id_version 
        FROM versiones_documento 
        WHERE id_documento = %s AND numero_version = %s
        """
        version_result = execute_query(version_query, (document_id, version_actual))
        
        if not version_result:
            return False, f"Versión actual {version_actual} no encontrada"
        
        return True, f"Consistencia validada para documento {document_id}, versión {version_actual}"
        
    except Exception as e:
        logger.error(f"Error validando consistencia: {str(e)}")
        return False, str(e)

def get_analysis_id_for_document(document_id, version_id=None):
    """
    Obtiene el analysis_id existente para un documento/versión
    """
    try:
        if version_id:
            query = """
            SELECT id_analisis FROM analisis_documento_ia 
            WHERE id_documento = %s AND id_version = %s
            ORDER BY fecha_analisis DESC 
            LIMIT 1
            """
            result = execute_query(query, (document_id, version_id))
        else:
            query = """
            SELECT id_analisis FROM analisis_documento_ia 
            WHERE id_documento = %s
            ORDER BY fecha_analisis DESC 
            LIMIT 1
            """
            result = execute_query(query, (document_id,))
        
        if result:
            return result[0]['id_analisis']
        return None
        
    except Exception as e:
        logger.error(f"Error obteniendo analysis_id: {str(e)}")
        return None

def create_analysis_record_for_version(document_id, version_id, analysis_id):
    """
    Crea un registro de análisis básico para una versión
    """
    try:
        analysis_data = {
            'id_analisis': analysis_id,
            'id_documento': document_id,
            'id_version': version_id,
            'tipo_documento': 'documento',
            'confianza_clasificacion': 0.5,
            'texto_extraido': None,
            'entidades_detectadas': None,
            'metadatos_extraccion': json.dumps({'created_for': 'pypdf2_processing'}),
            'fecha_analisis': datetime.now().isoformat(),
            'estado_analisis': 'iniciado',
            'mensaje_error': None,
            'version_modelo': 'pypdf2-basic',
            'tiempo_procesamiento': 0,
            'procesado_por': 'upload_processor',
            'requiere_verificacion': True,
            'verificado': False,
            'verificado_por': None,
            'fecha_verificacion': None
        }
        
        insert_analysis_record(analysis_data)
        logger.info(f"Registro de análisis creado: {analysis_id}")
        return True
        
    except Exception as e:
        logger.error(f"Error creando registro de análisis: {str(e)}")
        return False


def lambda_handler(event, context):
    """
    FUNCIÓN PRINCIPAL CORREGIDA: Maneja callbacks de Textract de manera robusta
    """
    callback_registro_id = log_document_processing_start(
        'textract_callback_robusto',
        'procesar_callback_textract_v2',
        datos_entrada={"sns_records": len(event.get('Records', []))}
    )
    
    try:
        logger.info(f"📨 Callback de Textract recibido")
        
        # Validar estructura del evento
        if not event.get('Records') or len(event['Records']) == 0:
            raise ValueError("Evento SNS inválido o vacío")
        
        # Extraer mensaje SNS
        sns_record = event['Records'][0]
        if 'Sns' not in sns_record or 'Message' not in sns_record['Sns']:
            raise ValueError("Estructura de mensaje SNS inválida")
        
        sns_message = sns_record['Sns']['Message']
        message_json = json.loads(sns_message)
        
        # Extraer información del job
        job_id = message_json.get('JobId')
        status = message_json.get('Status')
        job_tag = message_json.get('JobTag', '')
        
        if not job_id or not status:
            raise ValueError(f"JobId o Status faltantes en el mensaje: {message_json}")
        
        logger.info(f"📋 Job procesado: JobId={job_id}, Status={status}, JobTag={job_tag}")
        
        # Extraer document_id del job_tag
        document_id = get_document_id_from_job_tag(job_tag)
        if not document_id:
            raise ValueError(f"No se pudo extraer document_id del JobTag: {job_tag}")
        
        # Registrar procesamiento del documento específico
        doc_registro_id = log_document_processing_start(
            document_id,
            'callback_documento_especifico',
            datos_entrada={"job_id": job_id, "status": status},
            analisis_id=callback_registro_id
        )
        
        if status == 'SUCCEEDED':
            logger.info(f"✅ Procesamiento exitoso para documento {document_id}")
            
            # Validar consistencia del documento
            consistency_valid, consistency_msg = validate_document_version_consistency(document_id)
            if not consistency_valid:
                logger.warning(f"⚠️ Inconsistencia detectada: {consistency_msg}")
            
            # Obtener información del documento
            doc_info = get_document_by_id(document_id)
            if not doc_info:
                raise ValueError(f"Documento {document_id} no encontrado en base de datos")
            
            version_actual = doc_info.get('version_actual', 1)
            version_id = get_version_id(document_id, version_actual)
            
            if not version_id:
                logger.warning(f"⚠️ Version ID no encontrado, generando temporal")
                version_id = generate_uuid()
            
            logger.info(f"📄 Documento: {document_id}, Versión: {version_actual}, Version ID: {version_id}")
            
            # Obtener resultados de Textract
            textract_get_id = log_document_processing_start(
                document_id,
                'obtener_resultados_textract_v2',
                datos_entrada={"job_id": job_id},
                analisis_id=doc_registro_id
            )
            
            job_type = 'DOCUMENT_TEXT_DETECTION' if '_detection' in job_tag else 'DOCUMENT_ANALYSIS'
            textract_results, get_time = get_textract_results_safe(job_id, job_type)
            
            if not textract_results:
                raise ValueError(f"No se pudieron obtener resultados para JobId: {job_id}")
            
            log_document_processing_end(
                textract_get_id,
                estado='completado',
                datos_procesados={"pages_obtained": len(textract_results)},
                duracion_ms=int(get_time * 1000)
            )
            
            # Extracción completa de datos
            extraction_id = log_document_processing_start(
                document_id,
                'extraccion_completa_v2',
                datos_entrada={"pages_count": len(textract_results)},
                analisis_id=doc_registro_id
            )
            
            logger.info(f"🔄 Iniciando extracción completa de datos...")
            all_extracted_data = extract_complete_data_from_textract_fixed(textract_results)
            
            if not all_extracted_data['extraction_success']:
                logger.warning(f"⚠️ Extracción parcialmente exitosa")
            
            log_document_processing_end(
                extraction_id,
                estado='completado',
                datos_procesados={
                    "extraction_success": all_extracted_data['extraction_success'],
                    "text_length": len(all_extracted_data.get('full_text', '')),
                    "confidence": all_extracted_data.get('extraction_confidence', 0)
                },
                duracion_ms=int(all_extracted_data.get('processing_time', 0) * 1000)
            )
            
            # Obtener o crear analysis_id
            analysis_id = get_analysis_id_for_document(document_id, version_id)
            if not analysis_id:
                analysis_id = generate_uuid()
                create_analysis_record_for_version(document_id, version_id, analysis_id)
                logger.info(f"➕ Nuevo analysis_id creado: {analysis_id}")
            
            # Actualizar base de datos
            db_update_id = log_document_processing_start(
                document_id,
                'actualizar_bd_v2',
                datos_entrada={"analysis_id": analysis_id},
                analisis_id=doc_registro_id
            )
            
            try:
                # Actualizar análisis
                confidence = all_extracted_data.get('extraction_confidence', 0.5)
                doc_type = all_extracted_data.get('doc_type_info', {}).get('document_type', 'documento')
                
                update_success = update_analysis_record(
                    analysis_id,
                    all_extracted_data.get('full_text', ''),
                    json.dumps(all_extracted_data.get('structured_data', {})),
                    json.dumps({
                        'job_id': job_id,
                        'extraction_metadata': all_extracted_data.get('metadata', {}),
                        'query_answers': all_extracted_data.get('query_answers', {}),
                        'extraction_confidence': confidence,
                        'version_info': {'version_actual': version_actual, 'version_id': version_id}
                    }),
                    'textract_completado_v2',
                    f"textract-{job_type}",
                    int(get_time * 1000),
                    'textract_callback_v2',
                    confidence < 0.85,
                    False,
                    mensaje_error=all_extracted_data.get('error_message'),
                    confianza_clasificacion=confidence,
                    tipo_documento=doc_type,
                    id_version=version_id
                )
                
                if not update_success:
                    logger.warning("⚠️ Update analysis falló, pero continuando...")
                
                # Actualizar documento
                extraction_summary = {
                    'document_type': doc_type,
                    'confidence': confidence,
                    'extraction_success': all_extracted_data['extraction_success'],
                    'tables_count': len(all_extracted_data.get('tables', [])),
                    'forms_count': len(all_extracted_data.get('forms', {})),
                    'queries_count': len(all_extracted_data.get('query_answers', {})),
                    'version_info': {'version_actual': version_actual, 'version_id': version_id}
                }
                
                update_document_extraction_data(
                    document_id,
                    extraction_summary,
                    confidence,
                    False
                )
                
                # Actualizar estado
                update_document_processing_status(
                    document_id,
                    'textract_completado_v2',
                    f"Extracción completada con confianza {confidence:.2f}",
                    tipo_documento=doc_type
                )
                
                log_document_processing_end(
                    db_update_id,
                    estado='completado',
                    datos_procesados={"analysis_updated": update_success, "document_updated": True}
                )
                
                # Enviar a procesador específico
                queue_url = determine_processor_queue(doc_type)
                
                message = {
                    'document_id': document_id,
                    'document_type': doc_type,
                    'confidence': confidence,
                    'extraction_complete': True,
                    'version_info': {
                        'version_actual': version_actual,
                        'version_id': version_id,
                        'analysis_id': analysis_id
                    },
                    'processing_metadata': {
                        'job_id': job_id,
                        'extraction_success': all_extracted_data['extraction_success']
                    }
                }
                
                sqs_client.send_message(
                    QueueUrl=queue_url,
                    MessageBody=json.dumps(message)
                )
                
                logger.info(f"📤 Documento enviado a procesador: {queue_url}")
                
                log_document_processing_end(
                    doc_registro_id,
                    estado='completado',
                    datos_procesados={
                        "document_type": doc_type,
                        "confidence": confidence,
                        "next_queue": queue_url
                    }
                )
                
            except Exception as db_error:
                logger.error(f"❌ Error en actualización de BD: {str(db_error)}")
                
                log_document_processing_end(
                    db_update_id,
                    estado='error',
                    mensaje_error=str(db_error)
                )
                
                # Actualizar al menos el estado
                update_document_processing_status(
                    document_id,
                    'error_actualizacion_bd',
                    f"Error actualizando BD: {str(db_error)}"
                )
                
                log_document_processing_end(
                    doc_registro_id,
                    estado='error',
                    mensaje_error=f"Error BD: {str(db_error)}"
                )
        
        elif status == 'FAILED':
            error_message = f"Textract falló para JobId: {job_id}"
            logger.error(f"❌ {error_message}")
            
            update_document_processing_status(
                document_id,
                'textract_fallido',
                error_message
            )
            
            log_document_processing_end(
                doc_registro_id,
                estado='error',
                mensaje_error=error_message
            )
        
        else:
            logger.warning(f"⚠️ Estado inesperado: {status}")
            
            log_document_processing_end(
                doc_registro_id,
                estado='advertencia',
                mensaje_error=f"Estado inesperado: {status}"
            )
        
        # Finalizar procesamiento exitoso
        log_document_processing_end(
            callback_registro_id,
            estado='completado',
            datos_procesados={
                "document_id": document_id,
                "status": status,
                "job_id": job_id,
                "processed_successfully": status == 'SUCCEEDED'
            }
        )
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Callback procesado exitosamente',
                'document_id': document_id,
                'status': status,
                'job_id': job_id,
                'timestamp': datetime.now().isoformat()
            })
        }
        
    except Exception as e:
        logger.error(f"❌ Error crítico en callback: {str(e)}")
        import traceback
        logger.error(f"📍 Stack trace: {traceback.format_exc()}")
        
        # Intentar actualizar estado del documento si es posible
        try:
            if 'document_id' in locals() and document_id:
                update_document_processing_status(
                    document_id,
                    'error_callback',
                    f"Error en callback: {str(e)}"
                )
        except Exception:
            logger.error("No se pudo actualizar estado del documento")
        
        log_document_processing_end(
            callback_registro_id,
            estado='error',
            mensaje_error=str(e)
        )
        
        return {
            'statusCode': 500,
            'body': json.dumps({
                'error': 'Error procesando callback de Textract',
                'message': str(e),
                'timestamp': datetime.now().isoformat()
            })
        }