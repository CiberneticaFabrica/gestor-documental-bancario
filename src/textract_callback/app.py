# src/textract_callback/app.py
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
PROCESSED_BUCKET = os.environ.get('PROCESSED_BUCKET')

# Importar funciones necesarias
try:
    from common.db_connector import (
        update_document_processing_status, 
        update_document_extraction_data,
        update_analysis_record,
        get_document_type_by_id,
        get_document_by_id
    )
    from common.validation import guess_document_type
    logger.info("Módulos importados correctamente")
except ImportError as e:
    logger.error(f"Error importando módulos: {str(e)}")

def get_document_id_from_job_tag(job_tag):
    """Extrae el ID del documento desde el job tag"""
    parts = job_tag.split('_')
    if len(parts) > 0:
        return parts[0]
    return job_tag

def get_document_type_from_job_tag(job_tag):
    """Extrae el tipo de procesamiento desde el job tag"""
    if '_contract_analysis' in job_tag:
        return 'contract'
    elif '_id_analysis' in job_tag:
        return 'id'
    elif '_financial_analysis' in job_tag:
        return 'financial'
    else:
        return 'general'

def get_textract_results(job_id, job_type='DOCUMENT_ANALYSIS'):
    """Obtiene los resultados completos de Textract para un trabajo asíncrono"""
    pages = []
    start_time = time.time()
    
    try:
        logger.info(f"Obteniendo resultados de Textract para job_id {job_id}, tipo {job_type}")
        
        if job_type == 'DOCUMENT_ANALYSIS':
            # Obtener la primera página de resultados
            response = textract_client.get_document_analysis(JobId=job_id)
            pages.append(response)
            
            # Obtener las páginas restantes si hay paginación
            next_token = response.get('NextToken')
            while next_token:
                response = textract_client.get_document_analysis(
                    JobId=job_id,
                    NextToken=next_token
                )
                pages.append(response)
                next_token = response.get('NextToken')
                
        elif job_type == 'DOCUMENT_TEXT_DETECTION':
            # Obtener la primera página de resultados
            response = textract_client.get_document_text_detection(JobId=job_id)
            pages.append(response)
            
            # Obtener las páginas restantes si hay paginación
            next_token = response.get('NextToken')
            while next_token:
                response = textract_client.get_document_text_detection(
                    JobId=job_id,
                    NextToken=next_token
                )
                pages.append(response)
                next_token = response.get('NextToken')
        
        logger.info(f"Resultados obtenidos: {len(pages)} páginas de respuesta")
        processing_time = time.time() - start_time
        logger.info(f"Tiempo de obtención de resultados: {processing_time:.2f} segundos")
        return pages, processing_time
        
    except Exception as e:
        logger.error(f"Error al obtener resultados de Textract: {str(e)}")
        return [], 0

def extract_complete_data_from_textract(textract_results):
    """
    Extrae TODOS los datos posibles de los resultados de Textract en un solo procesamiento.
    Incluye texto, tablas, formularios, entidades y datos estructurados.
    """
    start_time = time.time()
    logger.info(f"Iniciando extracción completa de datos de Textract")
    
    # 1. Preparación: Obtener todos los bloques de todas las páginas
    all_blocks = []
    for page in textract_results:
        all_blocks.extend(page.get('Blocks', []))
    
    logger.info(f"Total de bloques obtenidos: {len(all_blocks)}")
    
    # Obtener metadata general
    doc_metadata = {
        'blocks_count': len(all_blocks),
        'page_count': max([block.get('Page', 0) for block in all_blocks if 'Page' in block] or [0]),
        'detection_time': textract_results[0].get('DetectDocumentTextModelVersion', 
                                                 textract_results[0].get('DocumentMetadata', {}).get('Version', 'unknown'))
    }
    
    # 2. Extracción de texto completo (incluye todo el texto del documento)
    text_blocks = []
    for block in all_blocks:
        if block.get('BlockType') == 'LINE' and 'Text' in block:
            # Guardar texto con información de página si está disponible
            page_num = block.get('Page', 1)
            confidence = block.get('Confidence', 0)
            text = block.get('Text', '')
            text_blocks.append({
                'text': text,
                'page': page_num,
                'confidence': confidence,
                'id': block.get('Id')
            })
    
    # Ordenar por página y crear texto completo
    text_blocks.sort(key=lambda x: x['page'])
    full_text = ' '.join([block['text'] for block in text_blocks])
    
    # 3. Extracción de tablas (todas las tablas con sus filas y columnas)
    tables = []
    table_blocks = [block for block in all_blocks if block.get('BlockType') == 'TABLE']
    
    for table_block in table_blocks:
        table_id = table_block.get('Id')
        page_num = table_block.get('Page', 1)
        
        # Encontrar todas las celdas asociadas a esta tabla
        cell_blocks = [
            block for block in all_blocks 
            if block.get('BlockType') == 'CELL' and 
            any(rel.get('Type') == 'CHILD' and table_id in rel.get('Ids', [])
                for rel in block.get('Relationships', []))
        ]
        
        # Si no encontramos celdas usando la relación, buscar por relación inversa
        if not cell_blocks:
            cell_blocks = [
                block for block in all_blocks
                if block.get('BlockType') == 'CELL' and block.get('Page') == page_num
            ]
        
        # Determinar tamaño de la tabla
        if cell_blocks:
            max_row = max([cell.get('RowIndex', 0) for cell in cell_blocks])
            max_col = max([cell.get('ColumnIndex', 0) for cell in cell_blocks])
            
            # Crear matriz vacía para la tabla
            table_data = []
            for _ in range(max_row):
                table_data.append([''] * max_col)
            
            # Rellenar la tabla con datos
            for cell in cell_blocks:
                row_idx = cell.get('RowIndex', 1) - 1
                col_idx = cell.get('ColumnIndex', 1) - 1
                
                # Obtener texto de la celda
                cell_text = ''
                
                # Método 1: Obtener texto de relaciones de tipo CHILD
                if 'Relationships' in cell:
                    for rel in cell.get('Relationships', []):
                        if rel.get('Type') == 'CHILD':
                            child_ids = rel.get('Ids', [])
                            
                            # Encontrar bloques WORD dentro de la celda
                            word_blocks = [
                                block for block in all_blocks
                                if block.get('Id') in child_ids and block.get('BlockType') == 'WORD'
                            ]
                            
                            # Concatenar palabras
                            if word_blocks:
                                cell_text = ' '.join([word.get('Text', '') for word in word_blocks])
                
                # Método 2: Si no hay texto por relaciones, usar el texto directo si existe
                if not cell_text and 'Text' in cell:
                    cell_text = cell.get('Text', '')
                
                # Asignar texto a la celda en la matriz
                if 0 <= row_idx < len(table_data) and 0 <= col_idx < len(table_data[row_idx]):
                    table_data[row_idx][col_idx] = cell_text
            
            # Agregar la tabla procesada a la lista
            tables.append({
                'id': table_id,
                'page': page_num,
                'rows': max_row,
                'columns': max_col,
                'data': table_data
            })
    
    # 4. Extracción de formularios (clave-valor)
    forms = {}
    
    # Identificar bloques de tipo KEY_VALUE_SET
    key_blocks = [
        block for block in all_blocks
        if block.get('BlockType') == 'KEY_VALUE_SET' and 
        'EntityTypes' in block and 'KEY' in block.get('EntityTypes', [])
    ]
    
    for key_block in key_blocks:
        key_id = key_block.get('Id')
        
        # Obtener texto de la clave
        key_text = ''
        
        # Buscar por relaciones CHILD para encontrar palabras de la clave
        for rel in key_block.get('Relationships', []):
            if rel.get('Type') == 'CHILD':
                child_ids = rel.get('Ids', [])
                
                # Obtener bloques WORD para la clave
                key_words = [
                    block.get('Text', '') for block in all_blocks
                    if block.get('Id') in child_ids and block.get('BlockType') == 'WORD'
                ]
                
                key_text = ' '.join(key_words)
        
        # Si no hay texto de clave, continuar
        if not key_text:
            continue
        
        # Buscar el valor asociado a esta clave
        value_text = ''
        
        # Obtener relaciones VALUE de la clave
        value_ids = []
        for rel in key_block.get('Relationships', []):
            if rel.get('Type') == 'VALUE':
                value_ids.extend(rel.get('Ids', []))
        
        # Procesar cada bloque de valor
        for value_id in value_ids:
            value_block = next((b for b in all_blocks if b.get('Id') == value_id), None)
            
            if value_block:
                # Obtener palabras del valor mediante relaciones CHILD
                for rel in value_block.get('Relationships', []):
                    if rel.get('Type') == 'CHILD':
                        value_word_ids = rel.get('Ids', [])
                        
                        # Obtener bloques WORD para el valor
                        value_words = [
                            block.get('Text', '') for block in all_blocks
                            if block.get('Id') in value_word_ids and block.get('BlockType') == 'WORD'
                        ]
                        
                        value_text = ' '.join(value_words)
        
        # Guardar par clave-valor
        key_text = key_text.strip()
        value_text = value_text.strip()
        
        if key_text:
            forms[key_text] = value_text
    
    # 5. Extracción de datos estructurados mediante patrones
    # Detectar patrones comunes en documentos bancarios
    
    # Patrones de entidades clave
    patterns = {
        'dni': r'\b\d{8}[A-Za-z]?\b',
        'passport': r'\b[A-Z]{1,2}[0-9]{6,7}\b',
        'email': r'\b[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}\b',
        'phone': r'\b(?:\+\d{1,3}\s?)?\(?\d{1,4}\)?[\s.-]?\d{3}[\s.-]?\d{4}\b',
        'iban': r'\b[A-Z]{2}\d{2}[A-Z0-9]{4}\d{7}([A-Z0-9]?){0,16}\b',
        'credit_card': r'\b(?:\d[ -]*?){13,16}\b',
        'dates': r'\b\d{1,2}[/.-]\d{1,2}[/.-]\d{2,4}\b',
        'amounts': r'\b\d{1,3}(?:[.,]\d{3})*(?:[.,]\d{2})?\s*(?:€|EUR|USD|\$)?\b',
        'percentages': r'\b\d{1,2}(?:[.,]\d{1,2})?\s*%\b'
    }
    
    # Buscar coincidencias de patrones en el texto completo
    structured_data = {}
    for entity_type, pattern in patterns.items():
        matches = re.findall(pattern, full_text)
        if matches:
            structured_data[entity_type] = matches
    
    # 6. Identificar tipo de documento basado en el contenido extraído
    # Usar función de clasificación basada en contenido
    doc_type_info = guess_document_type(full_text)
    
    # 7. Extraer información específica según tipo de documento
    specific_data = extract_type_specific_data(full_text, forms, structured_data, doc_type_info['document_type'])
    
    # 8. Componer resultado final con TODOS los datos extraídos
    result = {
        'metadata': doc_metadata,
        'full_text': full_text,
        'text_blocks': text_blocks,
        'tables': tables,
        'forms': forms,
        'structured_data': structured_data,
        'specific_data': specific_data,
        'doc_type_info': doc_type_info,
        'processing_time': time.time() - start_time
    }
    
    logger.info(f"Extracción completa finalizada. Tiempo total: {result['processing_time']:.2f} segundos")
    logger.info(f"Datos extraídos: {len(full_text)} caracteres de texto, {len(tables)} tablas, {len(forms)} campos de formulario")
    
    return result

def extract_type_specific_data(text, forms, structured_data, doc_type):
    """
    Extrae datos específicos según el tipo de documento.
    Esta función centraliza la extracción de datos para cada tipo de documento específico.
    """
    result = {}
    
    # Procesar según tipo de documento
    if doc_type == 'dni':
        # Extraer datos específicos de DNI
        result = extract_id_document_data(text, forms, structured_data)
    
    elif doc_type == 'pasaporte':
        # Extraer datos específicos de pasaporte
        result = extract_id_document_data(text, forms, structured_data, is_passport=True)
    
    elif doc_type in ['contrato', 'contrato_cuenta', 'contrato_tarjeta']:
        # Extraer datos específicos de contratos
        result = extract_contract_data(text, forms, structured_data)
    
    elif doc_type in ['extracto', 'extracto_bancario']:
        # Extraer datos específicos de extractos bancarios
        result = extract_financial_statement_data(text, forms, structured_data)
    
    elif doc_type in ['nomina', 'payroll']:
        # Extraer datos específicos de nóminas
        result = extract_payroll_data(text, forms, structured_data)
    
    elif doc_type in ['impuesto', 'declaracion_impuestos']:
        # Extraer datos específicos de declaraciones de impuestos
        result = extract_tax_data(text, forms, structured_data)
    
    # Otros tipos de documentos se añadirían aquí
    
    return result

def extract_id_document_data(text, forms, structured_data, is_passport=False):
    """Extrae información específica de documentos de identidad"""
    data = {
        'tipo_identificacion': 'pasaporte' if is_passport else 'dni',
        'numero_identificacion': None,
        'nombre_completo': None,
        'fecha_nacimiento': None,
        'fecha_emision': None,
        'fecha_expiracion': None,
        'pais_emision': None,
        'genero': None
    }
    
    # Buscar en formularios por campos relevantes (más confiable)
    form_mappings = {
        'nombre': ['nombre', 'apellidos', 'apellidos y nombre', 'name', 'full name', 'surname', 'given name'],
        'numero': ['documento', 'número', 'dni', 'nie', 'nif', 'passport', 'passport no', 'número de documento'],
        'emision': ['fecha de expedición', 'expedición', 'fecha emisión', 'date of issue', 'emisión'],
        'caducidad': ['fecha de caducidad', 'caducidad', 'validez', 'válido hasta', 'date of expiry', 'expiry date'],
        'nacimiento': ['fecha de nacimiento', 'nacimiento', 'date of birth', 'birth'],
        'pais': ['país', 'nacionalidad', 'country', 'nationality', 'issuing country']
    }
    
    # Buscar posibles claves en el formulario
    for field, possible_keys in form_mappings.items():
        for key in possible_keys:
            for form_key in forms:
                if key in form_key.lower():
                    value = forms[form_key]
                    
                    if field == 'nombre' and not data['nombre_completo']:
                        data['nombre_completo'] = value
                    elif field == 'numero' and not data['numero_identificacion']:
                        data['numero_identificacion'] = value
                    elif field == 'emision' and not data['fecha_emision']:
                        data['fecha_emision'] = normalize_date(value)
                    elif field == 'caducidad' and not data['fecha_expiracion']:
                        data['fecha_expiracion'] = normalize_date(value)
                    elif field == 'nacimiento' and not data['fecha_nacimiento']:
                        data['fecha_nacimiento'] = normalize_date(value)
                    elif field == 'pais' and not data['pais_emision']:
                        data['pais_emision'] = value
    
    # Si no tenemos datos de formularios, usar patrones en texto y datos estructurados
    
    # Número de documento
    if not data['numero_identificacion']:
        if is_passport and 'passport' in structured_data:
            data['numero_identificacion'] = structured_data['passport'][0]
        elif not is_passport and 'dni' in structured_data:
            data['numero_identificacion'] = structured_data['dni'][0]
    
    # Fechas
    if not data['fecha_emision'] or not data['fecha_expiracion']:
        if 'dates' in structured_data and len(structured_data['dates']) >= 2:
            # Ordenar fechas encontradas
            dates = [normalize_date(d) for d in structured_data['dates']]
            valid_dates = [d for d in dates if d]  # Filtrar fechas nulas
            
            if valid_dates:
                valid_dates.sort()
                
                # Generalmente, la primera es emisión y la última expiración
                if not data['fecha_emision'] and len(valid_dates) > 0:
                    data['fecha_emision'] = valid_dates[0]
                
                if not data['fecha_expiracion'] and len(valid_dates) > 1:
                    data['fecha_expiracion'] = valid_dates[-1]
    
    # Nombre completo (búsqueda en texto si no está en formularios)
    if not data['nombre_completo']:
        # Patrones comunes en documentos de identidad
        nombre_patterns = [
            r'APELLIDOS Y NOMBRE[:\s]+([A-ZÁÉÍÓÚÑ\s]+\s+[A-ZÁÉÍÓÚÑ\s]+)',
            r'APELLIDOS?[:\s]+([A-ZÁÉÍÓÚÑ\s]+)\s+NOMBRES?[:\s]+([A-ZÁÉÍÓÚÑ\s]+)',
            r'NOMBRE[:\s]+([A-ZÁÉÍÓÚÑ\s]+\s+[A-ZÁÉÍÓÚÑ\s]+)'
        ]
        
        for pattern in nombre_patterns:
            matches = re.search(pattern, text.upper())
            if matches:
                if len(matches.groups()) == 1:
                    data['nombre_completo'] = matches.group(1).strip()
                    break
                elif len(matches.groups()) == 2:
                    data['nombre_completo'] = f"{matches.group(1).strip()} {matches.group(2).strip()}"
                    break
    
    # País de emisión (si no está en formularios)
    if not data['pais_emision']:
        # Determinar por patrones comunes
        spain_patterns = ['ESPAÑA', 'SPAIN', 'REINO DE ESPAÑA', 'KINGDOM OF SPAIN']
        for pattern in spain_patterns:
            if pattern in text.upper():
                data['pais_emision'] = 'España'
                break
        
        if not data['pais_emision']:
            data['pais_emision'] = 'España'  # Valor por defecto
    
    return data

def extract_contract_data(text, forms, structured_data):
    """Extrae información específica de contratos bancarios"""
    data = {
        'tipo_contrato': None,
        'numero_contrato': None,
        'fecha_contrato': None,
        'partes': [],
        'importe': None,
        'tasa_interes': None,
        'condiciones': [],
        'fechas_clave': {}
    }
    
    # Determinar tipo de contrato
    contract_types = {
        'cuenta': ['cuenta corriente', 'cuenta de ahorro', 'cuenta bancaria', 'apertura de cuenta'],
        'tarjeta': ['tarjeta de crédito', 'tarjeta de débito', 'contrato de tarjeta'],
        'prestamo': ['préstamo', 'préstamo personal', 'crédito', 'hipoteca'],
        'inversion': ['inversión', 'fondo de inversión', 'plan de pensiones']
    }
    
    for tipo, keywords in contract_types.items():
        for keyword in keywords:
            if keyword.lower() in text.lower():
                data['tipo_contrato'] = tipo
                break
        if data['tipo_contrato']:
            break
    
    # Si no se ha determinado, asignar tipo genérico
    if not data['tipo_contrato']:
        data['tipo_contrato'] = 'general'
    
    # Buscar número de contrato con patrones
    contract_num_patterns = [
        r'[Nn][úu]mero de [Cc]ontrato:?\s*([A-Z0-9-]+)',
        r'[Cc]ontrato [Nn][úu]mero:?\s*([A-Z0-9-]+)',
        r'[Rr]eferencia:?\s*([A-Z0-9-]+)',
        r'[Nn][°º]\s*[Cc]ontrato:?\s*([A-Z0-9-]+)'
    ]
    
    for pattern in contract_num_patterns:
        match = re.search(pattern, text)
        if match:
            data['numero_contrato'] = match.group(1).strip()
            break
    
    # Buscar fecha del contrato
    date_patterns = [
        r'[Ff]echa del [Cc]ontrato:?\s*(\d{1,2}[/.-]\d{1,2}[/.-]\d{2,4})',
        r'[Ff]irmado el:?\s*(\d{1,2}[/.-]\d{1,2}[/.-]\d{2,4})',
        r'[Ff]echa:?\s*(\d{1,2}[/.-]\d{1,2}[/.-]\d{2,4})'
    ]
    
    for pattern in date_patterns:
        match = re.search(pattern, text)
        if match:
            data['fecha_contrato'] = normalize_date(match.group(1))
            break
    
    # Alternativa: usar fechas estructuradas
    if not data['fecha_contrato'] and 'dates' in structured_data and structured_data['dates']:
        # Usar la primera fecha encontrada como aproximación
        data['fecha_contrato'] = normalize_date(structured_data['dates'][0])
    
    # Buscar importes/cantidades monetarias
    if 'amounts' in structured_data and structured_data['amounts']:
        # Usar el importe más grande como aproximación del valor del contrato
        amounts = []
        for amount_str in structured_data['amounts']:
            # Normalizar y convertir a float
            clean_amount = amount_str.replace('.', '').replace(',', '.').strip()
            clean_amount = re.sub(r'[€$]', '', clean_amount).strip()
            try:
                amount = float(clean_amount)
                amounts.append((amount, amount_str))
            except ValueError:
                continue
        
        if amounts:
            # Ordenar por valor y tomar el más alto
            amounts.sort(key=lambda x: x[0], reverse=True)
            data['importe'] = amounts[0][1]
    
    # Buscar tasas de interés
    if 'percentages' in structured_data and structured_data['percentages']:
        # Buscar patrones de interés
        for i, percentage in enumerate(structured_data['percentages']):
            if re.search(r'(inter[ée]s|tasa)', text.lower()[max(0, text.lower().find(percentage)-30):text.lower().find(percentage)]):
                data['tasa_interes'] = percentage
                break
        
        # Si no encuentra, tomar el primer porcentaje
        if not data['tasa_interes'] and structured_data['percentages']:
            data['tasa_interes'] = structured_data['percentages'][0]
    
    # Buscar partes del contrato (personas/entidades)
    bank_patterns = [
        r'(BANCO\s+[A-Z\s]+)',
        r'([A-Z][a-z]+\s+BANK)',
        r'(CAJA\s+[A-Z\s]+)'
    ]
    
    for pattern in bank_patterns:
        matches = re.findall(pattern, text.upper())
        if matches:
            data['partes'].append({'tipo': 'entidad', 'nombre': matches[0]})
            break
    
    # Buscar persona (cliente)
    person_patterns = [
        r'(D\.|DON|DOÑA|Sr\.|Sra\.)\s+([A-ZÁÉÍÓÚÑ\s]+)',
        r'CLIENTE:?\s+([A-ZÁÉÍÓÚÑ\s]+)'
    ]
    
    for pattern in person_patterns:
        matches = re.findall(pattern, text.upper())
        if matches:
            data['partes'].append({'tipo': 'cliente', 'nombre': matches[0][-1].strip()})
            break
    
    return data

def extract_financial_statement_data(text, forms, structured_data):
    """Extrae información específica de extractos bancarios"""
    data = {
        'entidad': None,
        'numero_cuenta': None,
        'titular': None,
        'periodo': None,
        'fecha_inicio': None,
        'fecha_fin': None,
        'saldo_inicial': None,
        'saldo_final': None,
        'movimientos': []
    }
    
    # Buscar entidad bancaria
    bank_patterns = [
        r'(BANCO\s+[A-Z\s]+)',
        r'([A-Z][a-z]+\s+BANK)',
        r'(CAJA\s+[A-Z\s]+)'
    ]
    
    for pattern in bank_patterns:
        matches = re.findall(pattern, text.upper())
        if matches:
            data['entidad'] = matches[0]
            break
    
    # Buscar número de cuenta (IBAN u otro formato)
    if 'iban' in structured_data and structured_data['iban']:
        data['numero_cuenta'] = structured_data['iban'][0]
    else:
        # Buscar otros formatos de cuenta
        account_patterns = [
            r'[Cc]uenta:?\s*(\d{4}\s*\d{4}\s*\d{2}\s*\d{10})',
            r'[Cc]uenta:?\s*(\d{4}[- ]\d{4}[- ]\d{2}[- ]\d{4})',
            r'[Nn][ºo°].?\s*[Cc]uenta:?\s*([A-Z0-9]+)'
        ]
        
        for pattern in account_patterns:
            match = re.search(pattern, text)
            if match:
                data['numero_cuenta'] = match.group(1).strip()
                break
    
    # Buscar titular
    holder_patterns = [
        r'[Tt]itular:?\s+([A-ZÁÉÍÓÚÑ\s]+)',
        r'[Cc]liente:?\s+([A-ZÁÉÍÓÚÑ\s]+)'
    ]
    
    for pattern in holder_patterns:
        match = re.search(pattern, text)
        if match:
            data['titular'] = match.group(1).strip()
            break
    
    # Buscar período del extracto
    period_patterns = [
        r'[Pp]er[ií]odo:?\s+([^\.]+)',
        r'[Ee]xtracto del:?\s+([^\.]+)',
        r'[Dd]el\s+(\d{1,2}[/.-]\d{1,2}[/.-]\d{2,4})\s+[Aa]l\s+(\d{1,2}[/.-]\d{1,2}[/.-]\d{2,4})'
    ]
    
    for pattern in period_patterns:
        match = re.search(pattern, text)
        if match:
            # Si el patrón captura fechas inicio y fin
            if len(match.groups()) == 2:
                data['fecha_inicio'] = normalize_date(match.group(1))
                data['fecha_fin'] = normalize_date(match.group(2))
                data['periodo'] = f"Del {data['fecha_inicio']} al {data['fecha_fin']}"
            else:
                # Si captura el período completo
                data['periodo'] = match.group(1).strip()
                
                # Intentar extraer fechas del período
                dates_in_period = re.findall(r'\d{1,2}[/.-]\d{1,2}[/.-]\d{2,4}', data['periodo'])
                if len(dates_in_period) >= 2:
                    data['fecha_inicio'] = normalize_date(dates_in_period[0])
                    data['fecha_fin'] = normalize_date(dates_in_period[-1])
            
            break
    
    # Si no se encontró período, usar fechas estructuradas
    if not data['periodo'] and 'dates' in structured_data and len(structured_data['dates']) >= 2:
        dates = [normalize_date(d) for d in structured_data['dates']]
        valid_dates = [d for d in dates if d]
        if len(valid_dates) >= 2:
            valid_dates.sort()
            data['fecha_inicio'] = valid_dates[0]
            data['fecha_fin'] = valid_dates[-1]
            data['periodo'] = f"Del {data['fecha_inicio']} al {data['fecha_fin']}"
    
    # Buscar saldos
    balance_patterns = [
        r'[Ss]aldo [Ii]nicial:?\s*([0-9.,]+)',
        r'[Ss]aldo [Aa]nterior:?\s*([0-9.,]+)'
    ]
    
    for pattern in balance_patterns:
        match = re.search(pattern, text)
        if match:
            data['saldo_inicial'] = match.group(1).strip()
            break
    
    final_balance_patterns = [
        r'[Ss]aldo [Ff]inal:?\s*([0-9.,]+)',
        r'[Ss]aldo [Aa]ctual:?\s*([0-9.,]+)',
        r'[Ss]aldo [Dd]isponible:?\s*([0-9.,]+)'
    ]
    
    for pattern in final_balance_patterns:
        match = re.search(pattern, text)
        if match:
            data['saldo_final'] = match.group(1).strip()
            break
    
    # Para movimientos, necesitaríamos las tablas del extracto
    # Esto ya se incluye en el extraction de tablas general
    
    return data

def extract_payroll_data(text, forms, structured_data):
    """Extrae información específica de nóminas"""
    data = {
        'empresa': None,
        'empleado': None,
        'periodo': None,
        'fecha_nomina': None,
        'salario_base': None,
        'deducciones': None,
        'total_neto': None,
        'conceptos': []
    }
    
    # Buscar empresa
    company_patterns = [
        r'[Ee]mpresa:?\s+([A-ZÁÉÍÓÚÑ\s,\.]+)',
        r'[Pp]agador:?\s+([A-ZÁÉÍÓÚÑ\s,\.]+)',
        r'[Rr]az[óo]n [Ss]ocial:?\s+([A-ZÁÉÍÓÚÑ\s,\.]+)'
    ]
    
    for pattern in company_patterns:
        match = re.search(pattern, text)
        if match:
            data['empresa'] = match.group(1).strip()
            break
    
    # Buscar empleado
    employee_patterns = [
        r'[Tt]rabajador:?\s+([A-ZÁÉÍÓÚÑ\s,\.]+)',
        r'[Ee]mpleado:?\s+([A-ZÁÉÍÓÚÑ\s,\.]+)',
        r'[Nn]ombre:?\s+([A-ZÁÉÍÓÚÑ\s,\.]+)'
    ]
    
    for pattern in employee_patterns:
        match = re.search(pattern, text)
        if match:
            data['empleado'] = match.group(1).strip()
            break
    
    # Buscar período de la nómina
    period_patterns = [
        r'[Pp]er[íi]odo:?\s+([^\.]+)',
        r'[Nn][óo]mina de:?\s+([^\.]+)',
        r'[Cc]orrespondiente a:?\s+([^\.]+)',
        r'[Mm]es:?\s+([A-Za-z]+)',
        r'[Dd]el\s+(\d{1,2}[/.-]\d{1,2}[/.-]\d{2,4})\s+[Aa]l\s+(\d{1,2}[/.-]\d{1,2}[/.-]\d{2,4})'
    ]
    
    for pattern in period_patterns:
        match = re.search(pattern, text)
        if match:
            if len(match.groups()) == 2:
                # Si captura fecha inicio y fin
                from_date = normalize_date(match.group(1))
                to_date = normalize_date(match.group(2))
                data['periodo'] = f"Del {from_date} al {to_date}"
            else:
                data['periodo'] = match.group(1).strip()
            break
    
    # Buscar fecha de la nómina
    if 'dates' in structured_data and structured_data['dates']:
        data['fecha_nomina'] = normalize_date(structured_data['dates'][0])
    
    # Buscar importes
    if 'amounts' in structured_data and structured_data['amounts']:
        # Ordenar por valor numérico (el más alto probablemente es el total)
        amounts = []
        for amount_str in structured_data['amounts']:
            # Normalizar y convertir a float
            clean_amount = amount_str.replace('.', '').replace(',', '.').strip()
            clean_amount = re.sub(r'[€$]', '', clean_amount).strip()
            try:
                amount = float(clean_amount)
                amounts.append((amount, amount_str))
            except ValueError:
                continue
        
        if amounts:
            # Ordenar por valor
            amounts.sort(key=lambda x: x[0], reverse=True)
            
            # El más alto probablemente es el total bruto o neto
            # Buscar contexto alrededor de este importe
            if len(amounts) > 0:
                highest_amount = amounts[0][1]
                index = text.find(highest_amount)
                context = text[max(0, index-50):min(len(text), index+100)]
                
                # Verificar si es total neto o bruto
                if re.search(r'(neto|líquido|a percibir)', context.lower()):
                    data['total_neto'] = highest_amount
                else:
                    # Buscar específicamente total neto
                    net_patterns = [
                        r'([Tt]otal [Nn]eto|[Ll][íi]quido|[Aa] [Pp]ercibir):?\s*([0-9.,]+)',
                        r'[Tt]otal [Aa] [Pp]ercibir:?\s*([0-9.,]+)'
                    ]
                    
                    for pattern in net_patterns:
                        match = re.search(pattern, text)
                        if match:
                            data['total_neto'] = match.group(1 if len(match.groups()) == 1 else 2).strip()
                            break
    
    # Buscar salario base
    base_patterns = [
        r'([Ss]alario [Bb]ase|[Ss]ueldo [Bb]ase):?\s*([0-9.,]+)',
        r'[Bb]ase:?\s*([0-9.,]+)'
    ]
    
    for pattern in base_patterns:
        match = re.search(pattern, text)
        if match:
            data['salario_base'] = match.group(1 if len(match.groups()) == 1 else 2).strip()
            break
    
    # Buscar deducciones
    deduction_patterns = [
        r'([Tt]otal [Dd]educciones|[Dd]educciones):?\s*([0-9.,]+)',
        r'[Dd]escuentos:?\s*([0-9.,]+)'
    ]
    
    for pattern in deduction_patterns:
        match = re.search(pattern, text)
        if match:
            data['deducciones'] = match.group(1 if len(match.groups()) == 1 else 2).strip()
            break
    
    return data

def extract_tax_data(text, forms, structured_data):
    """Extrae información específica de declaraciones de impuestos"""
    data = {
        'tipo_impuesto': None,
        'ejercicio': None,
        'contribuyente': None,
        'nif': None,
        'resultado': None,
        'a_ingresar': None,
        'a_devolver': None
    }
    
    # Determinar tipo de impuesto
    tax_types = {
        'irpf': ['irpf', 'impuesto sobre la renta', 'declaración de la renta', 'modelo 100'],
        'iva': ['iva', 'impuesto sobre el valor añadido', 'modelo 303', 'modelo 390'],
        'sociedades': ['impuesto sobre sociedades', 'modelo 200'],
        'patrimonio': ['impuesto sobre el patrimonio', 'modelo 714']
    }
    
    for tipo, keywords in tax_types.items():
        for keyword in keywords:
            if keyword.lower() in text.lower():
                data['tipo_impuesto'] = tipo
                break
        if data['tipo_impuesto']:
            break
    
    # Buscar ejercicio fiscal
    year_patterns = [
        r'[Ee]jercicio:?\s*(\d{4})',
        r'[Aa]ño:?\s*(\d{4})',
        r'[Pp]er[íi]odo:?\s*(\d{4})'
    ]
    
    for pattern in year_patterns:
        match = re.search(pattern, text)
        if match:
            data['ejercicio'] = match.group(1)
            break
    
    # Si no se encuentra, buscar años en el texto
    if not data['ejercicio']:
        years = re.findall(r'\b(20\d{2})\b', text)
        if years:
            # Usar el año que más aparece
            from collections import Counter
            year_count = Counter(years)
            data['ejercicio'] = year_count.most_common(1)[0][0]
    
    # Buscar contribuyente
    taxpayer_patterns = [
        r'[Cc]ontribuyente:?\s+([A-ZÁÉÍÓÚÑ\s,\.]+)',
        r'[Dd]eclarante:?\s+([A-ZÁÉÍÓÚÑ\s,\.]+)',
        r'[Aa]pellidos y [Nn]ombre:?\s+([A-ZÁÉÍÓÚÑ\s,\.]+)'
    ]
    
    for pattern in taxpayer_patterns:
        match = re.search(pattern, text)
        if match:
            data['contribuyente'] = match.group(1).strip()
            break
    
    # Buscar NIF
    if 'dni' in structured_data and structured_data['dni']:
        data['nif'] = structured_data['dni'][0]
    else:
        nif_patterns = [
            r'[Nn][Ii][Ff]:?\s*([0-9A-Z]{8,9})',
            r'[Dd][Nn][Ii]:?\s*([0-9A-Z]{8,9})'
        ]
        
        for pattern in nif_patterns:
            match = re.search(pattern, text)
            if match:
                data['nif'] = match.group(1).strip()
                break
    
    # Buscar resultado (a ingresar o devolver)
    result_patterns = [
        r'[Rr]esultado:?\s*([0-9.,]+)',
        r'[Cc]uota [Rr]esultante:?\s*([0-9.,]+)',
        r'[Rr]esultado de la [Dd]eclaración:?\s*([0-9.,]+)'
    ]
    
    for pattern in result_patterns:
        match = re.search(pattern, text)
        if match:
            data['resultado'] = match.group(1).strip()
            break
    
    # Buscar a ingresar
    pay_patterns = [
        r'[Aa] [Ii]ngresar:?\s*([0-9.,]+)',
        r'[Ii]mporte a [Ii]ngresar:?\s*([0-9.,]+)'
    ]
    
    for pattern in pay_patterns:
        match = re.search(pattern, text)
        if match:
            data['a_ingresar'] = match.group(1).strip()
            break
    
    # Buscar a devolver
    refund_patterns = [
        r'[Aa] [Dd]evolver:?\s*([0-9.,]+)',
        r'[Ii]mporte a [Dd]evolver:?\s*([0-9.,]+)'
    ]
    
    for pattern in refund_patterns:
        match = re.search(pattern, text)
        if match:
            data['a_devolver'] = match.group(1).strip()
            break
    
    return data

def normalize_date(date_str):
    """Normaliza un string de fecha a formato ISO (YYYY-MM-DD)"""
    if not date_str:
        return None
        
    try:
        # Eliminar caracteres no numéricos excepto separadores
        clean_date = re.sub(r'[^\d/.-]', '', date_str)
        
        # Detectar formato
        if re.match(r'\d{1,2}[/.-]\d{1,2}[/.-]\d{2,4}', clean_date):
            # Formato DD/MM/YYYY o DD/MM/YY
            parts = re.split(r'[/.-]', clean_date)
            
            day = int(parts[0])
            month = int(parts[1])
            year = int(parts[2])
            
            # Ajustar año si es de dos dígitos
            if year < 100:
                if year < 50:  # Heurística: asumimos que años < 50 son 20XX
                    year += 2000
                else:
                    year += 1900
            
            return f"{year:04d}-{month:02d}-{day:02d}"
            
        elif re.match(r'\d{4}[/.-]\d{1,2}[/.-]\d{1,2}', clean_date):
            # Formato YYYY/MM/DD
            parts = re.split(r'[/.-]', clean_date)
            
            year = int(parts[0])
            month = int(parts[1])
            day = int(parts[2])
            
            return f"{year:04d}-{month:02d}-{day:02d}"
            
        else:
            return None
            
    except (ValueError, IndexError):
        return None

def determine_processor_queue(doc_type):
    """Determina la cola SQS apropiada según el tipo de documento"""
    
    # Mapeo de tipos de documento a colas
    queue_mapping = {
        'dni': ID_PROCESSOR_QUEUE_URL,
        'pasaporte': ID_PROCESSOR_QUEUE_URL,
        'contrato': CONTRACT_PROCESSOR_QUEUE_URL,
        'contrato_cuenta': CONTRACT_PROCESSOR_QUEUE_URL,
        'contrato_tarjeta': CONTRACT_PROCESSOR_QUEUE_URL,
        'extracto': FINANCIAL_PROCESSOR_QUEUE_URL,
        'extracto_bancario': FINANCIAL_PROCESSOR_QUEUE_URL,
        'nomina': FINANCIAL_PROCESSOR_QUEUE_URL,
        'impuesto': FINANCIAL_PROCESSOR_QUEUE_URL
    }
    
    # Buscar cola específica o usar default
    for key, queue in queue_mapping.items():
        if doc_type.lower() == key or doc_type.lower().startswith(key):
            return queue
    
    # Si no hay coincidencia, usar la cola por defecto
    return DEFAULT_PROCESSOR_QUEUE_URL

def lambda_handler(event, context):
    """
    Función que maneja las notificaciones de finalización de Textract.
    Se activa por eventos SNS de Textract.
    """
    try:
        logger.info(f"Evento recibido: {json.dumps(event)}")
        
        # Extraer mensaje SNS
        sns_message = event['Records'][0]['Sns']['Message']
        message_json = json.loads(sns_message)
        
        job_id = message_json['JobId']
        status = message_json['Status']
        job_tag = message_json.get('JobTag', '')
        
        logger.info(f"Notificación de Textract: JobId={job_id}, Status={status}, JobTag={job_tag}")
        
        # Extraer ID del documento y tipo de análisis del JobTag
        document_id = get_document_id_from_job_tag(job_tag)
        doc_type = get_document_type_from_job_tag(job_tag)
        
        # Determinar tipo de trabajo
        job_type = 'DOCUMENT_ANALYSIS'  # Por defecto
        if '_textract_detection' in job_tag:
            job_type = 'DOCUMENT_TEXT_DETECTION'
        
        if status == 'SUCCEEDED':
            # Obtener resultados completos de Textract
            textract_results, processing_time = get_textract_results(job_id, job_type)
            
            if not textract_results:
                logger.error(f"No se pudieron obtener resultados para el Job {job_id}")
                update_document_processing_status(
                    document_id, 
                    'error',
                    f"No se pudieron obtener resultados para el Job {job_id}"
                )
                return {
                    'statusCode': 500,
                    'body': 'Error al obtener resultados de Textract'
                }
            
            # Extraer TODOS los datos posibles en un único proceso
            logger.info(f"Extrayendo datos completos para documento {document_id}")
            all_extracted_data = extract_complete_data_from_textract(textract_results)
            
            # Calcular confianza global
            confidence = all_extracted_data['doc_type_info']['confidence']
            doc_type_identified = all_extracted_data['doc_type_info']['document_type']
            
            # Registrar datos extraídos en base de datos
            try:
                # 1. Actualizar el registro de análisis con TODOS los datos extraídos
                update_analysis_record(
                    document_id,
                    all_extracted_data['full_text'],  # Texto completo extraído
                    json.dumps(all_extracted_data['structured_data']),  # Entidades detectadas
                    json.dumps({  # Metadatos de extracción
                        'job_id': job_id,
                        'job_type': job_type,
                        'tables_count': len(all_extracted_data['tables']),
                        'forms_count': len(all_extracted_data['forms']),
                        'text_blocks_count': len(all_extracted_data['text_blocks']),
                        'processing_time': all_extracted_data['processing_time']
                    }),
                    'textract_completado',  # Estado de análisis
                    f"textract-{job_type}",  # Versión del modelo
                    int(processing_time * 1000),  # Tiempo de procesamiento en ms
                    'textract_callback',  # Procesado por
                    confidence < 0.85,  # Requiere verificación si confianza baja
                    False,  # No verificado aún
                    mensaje_error=None,
                    confianza_clasificacion=confidence,
                    tipo_documento=doc_type_identified
                )
                
                # 2. Actualizar documento con todos los datos extraídos
                extraction_data = {
                    'document_type': doc_type_identified,
                    'confidence': confidence,
                    'text_sample': all_extracted_data['full_text'][:1000] if all_extracted_data['full_text'] else '',
                    'tables': all_extracted_data['tables'],
                    'forms': all_extracted_data['forms'],
                    'structured_data': all_extracted_data['structured_data'],
                    'specific_data': all_extracted_data['specific_data']
                }
                
                update_document_extraction_data(
                    document_id,
                    extraction_data,
                    confidence,
                    False  # No validado aún
                )
                
                logger.info(f"Datos extraídos guardados correctamente para documento {document_id}")
                
                # 3. Actualizar estado de procesamiento
                update_document_processing_status(
                    document_id, 
                    'textract_completado',
                    f"Extracción completada con confianza {confidence:.2f}"
                )
                
                # 4. Determinar la cola SQS para el siguiente paso de procesamiento
                # basado en el tipo de documento identificado
                queue_url = determine_processor_queue(doc_type_identified)
                
                # 5. Crear mensaje para el procesador específico
                # IMPORTANTE: Solo enviamos la referencia al documento, no los datos completos
                # Los datos ya están en la base de datos
                message = {
                    'document_id': document_id,
                    'document_type': doc_type_identified,
                    'confidence': confidence,
                    'extraction_complete': True,  # Indicar que ya se ha completado la extracción
                    'specific_data_available': bool(all_extracted_data['specific_data'])
                }
                
                # 6. Enviar a cola SQS
                sqs_client.send_message(
                    QueueUrl=queue_url,
                    MessageBody=json.dumps(message)
                )
                
                logger.info(f"Documento {document_id} enviado a procesador específico: {queue_url}")
                
            except Exception as db_error:
                logger.error(f"Error al guardar datos extraídos: {str(db_error)}")
                update_document_processing_status(
                    document_id, 
                    'error',
                    f"Error al guardar datos extraídos: {str(db_error)}"
                )
                
        else:
            # Actualizar estado en caso de error
            error_message = f"Error en el procesamiento Textract: {status}"
            update_document_processing_status(document_id, 'error', error_message)
            logger.error(error_message)
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'document_id': document_id,
                'status': status,
                'message': 'Procesamiento de notificación de Textract completado'
            })
        }
        
    except Exception as e:
        logger.error(f"Error procesando notificación de Textract: {str(e)}")
        
        # Intentar obtener document_id para actualizar estado
        try:
            document_id = get_document_id_from_job_tag(message_json.get('JobTag', ''))
            update_document_processing_status(
                document_id, 
                'error', 
                f"Error en callback de Textract: {str(e)}"
            )
        except Exception:
            logger.error("No se pudo actualizar estado del documento")
        
        return {
            'statusCode': 500,
            'body': json.dumps({
                'error': str(e),
                'message': 'Error procesando notificación de Textract'
            })
        }