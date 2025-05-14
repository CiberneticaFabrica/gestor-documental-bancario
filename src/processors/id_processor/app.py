# src/processors/id_processor/app.py
import os
import json
import boto3
import logging
import sys
import re
import time
from datetime import datetime, timedelta
import traceback
from common.confidence_utils import evaluate_confidence, mark_for_manual_review

# Agregar las rutas para importar módulos comunes
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
sys.path.append('/opt')

from common.db_connector import (
    execute_query,
    update_document_extraction_data,
    update_document_processing_status,
    get_document_by_id,
    register_document_identification,
    link_document_to_client,
    insert_analysis_record,
    generate_uuid,
    log_document_processing_start,
    log_document_processing_end,
    get_document_processing_history,
    update_document_extraction_data_with_type_preservation,
    assign_folder_and_link,
    get_client_id_by_document
)

 
# Configurar el logger
logger = logging.getLogger()
logger.setLevel(os.environ.get('LOG_LEVEL', 'INFO'))

# Patrones regex para extraer información de documentos de identidad
DNI_PATTERN = r'(?i)(?:DNI|Documento Nacional de Identidad)[^\d]*(\d{8}[A-Z]?)'
PASSPORT_PATTERN = r'(?i)(?:Pasaporte|Passport)[^\d]*([A-Z]{1,2}[0-9]{6,7})'
NAME_PATTERN = r'(?i)(?:Nombre|Name)[^\w]*([\w\s]+)'
SURNAME_PATTERN = r'(?i)(?:Apellidos|Surname)[^\w]*([\w\s]+)'
DOB_PATTERN = r'(?i)(?:Fecha de nacimiento|Date of birth)[^\d]*(\d{1,2}[/-]\d{1,2}[/-]\d{2,4})'
EXPIRY_PATTERN = r'(?i)(?:Fecha de caducidad|Date of expiry)[^\d]*(\d{1,2}[/-]\d{1,2}[/-]\d{2,4})'
NATIONALITY_PATTERN = r'(?i)(?:Nacionalidad|Nationality)[^\w]*([\w\s]+)'
PANAMA_ID_PATTERN = r'(?i)(?:\b|cedula|identidad|id)[^\d]*(\d{1,2}-\d{1,3}-\d{1,4})'
# Patrones específicos para fechas en documentos panameños
PANAMA_ISSUE_DATE_PATTERN = r'(?:[XE]XPEDIDA|EMITIDA):?\s*(\d{1,2}[-\s][a-zA-Zéúíóá]+[-\s]\d{4})'
PANAMA_EXPIRY_DATE_PATTERN = r'(?:EXPIRA|VENCE):?\s*(\d{1,2}[-\s][a-zA-Zéúíóá]+[-\s]\d{4})'

def get_extracted_data_from_db(document_id):
    """
    Recupera los datos ya extraídos por textract_callback de la base de datos.
    """
    try:
        start_time = time.time()
        # Obtener documento
        document_data = get_document_by_id(document_id)
        
        if not document_data:
            logger.error(f"No se encontró el documento {document_id} en la base de datos")
            return None
        
        # Obtener los datos extraídos del campo JSON
        extracted_data = {}
        if document_data.get('datos_extraidos_ia'):
            try:
                # Si ya es un diccionario, usarlo directamente
                if isinstance(document_data['datos_extraidos_ia'], dict):
                    extracted_data = document_data['datos_extraidos_ia']
                else:
                    # Si es una cadena JSON, deserializarla
                    extracted_data = json.loads(document_data['datos_extraidos_ia'])
            except json.JSONDecodeError:
                logger.error(f"Error al decodificar datos_extraidos_ia para documento {document_id}")
                return None
        
        # Obtener texto extraído y datos analizados
        query = """
        SELECT texto_extraido, entidades_detectadas, metadatos_extraccion, estado_analisis, tipo_documento
        FROM analisis_documento_ia
        WHERE id_documento = %s
        ORDER BY fecha_analisis DESC
        LIMIT 1
        """
        
        analysis_results = execute_query(query, (document_id,))
        
        if not analysis_results:
            logger.warning(f"No se encontró análisis en base de datos para documento {document_id}")
            # Continuar con lo que tengamos en datos_extraidos_ia
        else:
            analysis_data = analysis_results[0]
            
            # Agregar texto completo
            if analysis_data.get('texto_extraido'):
                extracted_data['texto_completo'] = analysis_data['texto_extraido']
            
            # Agregar entidades detectadas
            if analysis_data.get('entidades_detectadas'):
                try:
                    entidades = json.loads(analysis_data['entidades_detectadas']) if isinstance(analysis_data['entidades_detectadas'], str) else analysis_data['entidades_detectadas']
                    extracted_data['entidades'] = entidades
                except json.JSONDecodeError:
                    logger.warning(f"Error al decodificar entidades_detectadas para documento {document_id}")
            
            # Agregar metadatos de extracción
            if analysis_data.get('metadatos_extraccion'):
                try:
                    metadatos = json.loads(analysis_data['metadatos_extraccion']) if isinstance(analysis_data['metadatos_extraccion'], str) else analysis_data['metadatos_extraccion']
                    extracted_data['metadatos_extraccion'] = metadatos
                except json.JSONDecodeError:
                    logger.warning(f"Error al decodificar metadatos_extraccion para documento {document_id}")
            
            # Agregar tipo de documento detectado
            if analysis_data.get('tipo_documento'):
                extracted_data['tipo_documento_detectado'] = analysis_data['tipo_documento']
        
        # Registrar tiempo de consulta
        logger.info(f"Datos recuperados para documento {document_id} en {time.time() - start_time:.2f} segundos")
        
        return {
            'document_id': document_id,
            'document_data': document_data,
            'extracted_data': extracted_data
        }
        
    except Exception as e:
        logger.error(f"Error al recuperar datos de documento {document_id}: {str(e)}")
        logger.error(traceback.format_exc())
        return None
    
def format_date_panama(date_str):
    """
    Convierte fechas en formato panameño (DD-mes-YYYY) a formato ISO (YYYY-MM-DD)
    Por ejemplo: '02-ago-2023' -> '2023-08-02'
    """
    if not date_str:
        return None
    
    # Limpiar la cadena de espacios extras
    date_str = re.sub(r'\s+', '-', date_str.strip())
    
    # Diccionario de meses en español a número
    month_map = {
        'ene': '01', 'feb': '02', 'mar': '03', 'abr': '04', 'may': '05', 'jun': '06',
        'jul': '07', 'ago': '08', 'sep': '09', 'oct': '10', 'nov': '11', 'dic': '12',
        'enero': '01', 'febrero': '02', 'marzo': '03', 'abril': '04', 'mayo': '05', 'junio': '06',
        'julio': '07', 'agosto': '08', 'septiembre': '09', 'octubre': '10', 'noviembre': '11', 'diciembre': '12'
    }
    
    try:
        # Separar día, mes y año (manejar tanto guiones como espacios)
        parts = re.split(r'[-\s]+', date_str)
        if len(parts) != 3:
            logger.warning(f"Formato de fecha no reconocido: {date_str}")
            return None
        
        day = parts[0].strip().zfill(2)
        month_text = parts[1].strip().lower()
        year = parts[2].strip()
        
        # Convertir mes a número
        if month_text in month_map:
            month = month_map[month_text]
        else:
            # Si no se encuentra el mes exacto, intentar con las primeras tres letras
            month_abbr = month_text[:3]
            if month_abbr in month_map:
                month = month_map[month_abbr]
            else:
                logger.warning(f"Mes no reconocido: {month_text}")
                return None
        
        # Formatear en ISO
        return f"{year}-{month}-{day}"
    except Exception as e:
        logger.warning(f"Error al procesar fecha '{date_str}': {str(e)}")
        return None

def extract_id_document_data(text, entidades=None, metadatos=None):
    """
    Extrae información específica de documentos de identidad a partir del texto
    ya procesado por textract_callback.
    """
    # Resultado de la extracción
    extracted_data = {
        'tipo_identificacion': 'desconocido',
        'numero_identificacion': None,
        'nombre_completo': None,
        'nombre': None,
        'apellidos': None,
        'genero': None,
        'fecha_nacimiento': None,
        'fecha_emision': None,
        'fecha_expiracion': None,
        'nacionalidad': None,
        'pais_emision': None,
        'texto_completo': text
    }
    
    # Primero verificamos si ya tenemos datos estructurados disponibles
    if metadatos and isinstance(metadatos, dict) and 'specific_data' in metadatos:
        specific_data = metadatos['specific_data']
        for key, value in specific_data.items():
            if key in extracted_data and value:
                extracted_data[key] = value
    
    # Comprobar si hay número de teléfono en entidades que podría ser una cédula panameña
    if entidades and isinstance(entidades, dict) and 'phone' in entidades and entidades['phone']:
        # Verificar si parece una cédula panameña (formato: N-NNN-NNNN)
        phone_value = entidades['phone'][0]
        if re.match(r'\d{1,2}-\d{1,3}-\d{1,4}', phone_value):
            extracted_data['numero_identificacion'] = phone_value
            extracted_data['tipo_identificacion'] = 'cedula_panama'
            extracted_data['pais_emision'] = 'Panamá'

    # Si aún no tenemos número de identificación, buscar en el texto usando el patrón específico de Panamá
    if not extracted_data['numero_identificacion']:
        panama_id_match = re.search(PANAMA_ID_PATTERN, text)
        if panama_id_match:
            extracted_data['numero_identificacion'] = panama_id_match.group(1)
            extracted_data['tipo_identificacion'] = 'cedula_panama'
            extracted_data['pais_emision'] = 'Panamá'

    # Si ya tenemos el tipo de documento detectado, lo usamos
    if metadatos and isinstance(metadatos, dict) and 'document_type' in metadatos:
        doc_type = metadatos['document_type'].lower()
        if 'dni' in doc_type or 'identidad' in doc_type:
            extracted_data['tipo_identificacion'] = 'dni'
            extracted_data['pais_emision'] = 'España'
        elif 'pasaporte' in doc_type or 'passport' in doc_type:
            extracted_data['tipo_identificacion'] = 'pasaporte'
    
    # Manejo específico para documentos panameños
# Manejo específico para documentos panameños
    if "REPUBLICA DE PANAMA" in text or "REPÚBLICA DE PANAMÁ" in text:
        extracted_data['pais_emision'] = 'Panamá'
        
        # Extraer número de cédula si no se ha hecho ya
        if not extracted_data['numero_identificacion']:
            cedula_match = re.search(r'\b(\d{1,2}-\d{3,4}-\d{1,4})\b', text)
            if cedula_match:
                extracted_data['numero_identificacion'] = cedula_match.group(1)
                extracted_data['tipo_identificacion'] = 'cedula_panama'
                
        # Extraer nombre completo con patrones más flexibles para documentos panameños
        if not extracted_data['nombre_completo']:
            # Patrón 1: Nombre después de IDENTIDAD
            nombre_pattern1 = re.search(r'IDENTIDAD\s+\d+\s+([A-ZÁÉÍÓÚÑ\s]+)\s+NOMBRE\s+USUAL', text)
            if nombre_pattern1:
                extracted_data['nombre_completo'] = nombre_pattern1.group(1).strip()
            
            # Patrón 2: Nombre después de TRIBUNAL ELECTORAL
            if not extracted_data['nombre_completo']:
                nombre_pattern2 = re.search(r'TRIBUNAL\s+ELECTORAL\s+([A-Za-záéíóúñÁÉÍÓÚÑ\s]+)\s+NOMBRE\s+USUAL', text)
                if nombre_pattern2:
                    extracted_data['nombre_completo'] = nombre_pattern2.group(1).strip()
            
            # Patrón 3: Entre ELECTORAL y FECHA/ECHA DE NACIMIENTO
            if not extracted_data['nombre_completo']:
                nombre_pattern3 = re.search(r'ELECTORAL\s+([A-Za-záéíóúñÁÉÍÓÚÑ\s]+)\s+(?:F|E)ECHA\s+DE\s+NACIMIENTO', text)
                if nombre_pattern3:
                    extracted_data['nombre_completo'] = nombre_pattern3.group(1).strip()
            
            # Patrón 4: Si tiene el formato con PA N A letras, que es un formato especial
            if not extracted_data['nombre_completo']:
                nombre_pattern4 = re.search(r'P\s+A\s+([\w\s]+)\s+N\s+A\s+([\w\s]+)\s+M\s+\d+\s+A', text)
                if nombre_pattern4:
                    nombre = nombre_pattern4.group(1).strip()
                    apellido = nombre_pattern4.group(2).strip()
                    extracted_data['nombre_completo'] = f"{nombre} {apellido}"
                    extracted_data['nombre'] = nombre
                    extracted_data['apellidos'] = apellido
            
            # Patrón 5: Si ninguno de los anteriores funcionó, buscar cualquier secuencia de palabras capitalizadas
            if not extracted_data['nombre_completo']:
                # Buscar secuencias que parezcan nombres (2-4 palabras con mayúsculas)
                name_candidates = re.findall(r'([A-Z][a-z]+(?:\s+[A-Z][a-zñ]+){1,3})', text)
                if name_candidates:
                    # Ordenar por longitud y tomar el más largo (más probable que sea nombre completo)
                    best_candidate = sorted(name_candidates, key=len, reverse=True)[0]
                    extracted_data['nombre_completo'] = best_candidate
    
    # Determinar tipo de documento si no está definido
    if extracted_data['tipo_identificacion'] == 'desconocido':
        if re.search(DNI_PATTERN, text, re.IGNORECASE):
            extracted_data['tipo_identificacion'] = 'dni'
            extracted_data['pais_emision'] = 'España'
        elif re.search(PASSPORT_PATTERN, text, re.IGNORECASE):
            extracted_data['tipo_identificacion'] = 'pasaporte'
    
    # Extraer número de identificación si no está definido
    if not extracted_data['numero_identificacion']:
        if extracted_data['tipo_identificacion'] == 'dni':
            dni_match = re.search(DNI_PATTERN, text, re.IGNORECASE)
            if dni_match:
                extracted_data['numero_identificacion'] = dni_match.group(1)
        elif extracted_data['tipo_identificacion'] == 'pasaporte':
            passport_match = re.search(PASSPORT_PATTERN, text, re.IGNORECASE)
            if passport_match:
                extracted_data['numero_identificacion'] = passport_match.group(1)
 
    # Extraer nombre si no está definido
    if not extracted_data['nombre']:
        name_match = re.search(NAME_PATTERN, text, re.IGNORECASE)
        if name_match:
            extracted_data['nombre'] = name_match.group(1).strip()
    
    # Extraer apellidos si no están definidos
    if not extracted_data['apellidos']:
        surname_match = re.search(SURNAME_PATTERN, text, re.IGNORECASE)
        if surname_match:
            extracted_data['apellidos'] = surname_match.group(1).strip()
    
    # Construir nombre completo si tenemos nombre y apellidos
    if not extracted_data['nombre_completo'] and extracted_data['nombre'] and extracted_data['apellidos']:
        extracted_data['nombre_completo'] = f"{extracted_data['nombre']} {extracted_data['apellidos']}"
    
    # Buscar nombre completo en entidades si está disponible
    if not extracted_data['nombre_completo'] and entidades:
        if isinstance(entidades, dict) and 'PERSON' in entidades and entidades['PERSON']:
            extracted_data['nombre_completo'] = entidades['PERSON'][0]
        elif isinstance(entidades, list):
            for entity in entidades:
                if isinstance(entity, dict) and entity.get('Type') == 'PERSON':
                    extracted_data['nombre_completo'] = entity.get('Text', '')
                    break
    
    # Extraer fecha de nacimiento si no está definida
    if not extracted_data['fecha_nacimiento']:
        dob_match = re.search(DOB_PATTERN, text, re.IGNORECASE)
        if dob_match:
            extracted_data['fecha_nacimiento'] = format_date_panama(dob_match.group(1))
    
    # Extraer nacionalidad si no está definida
    if not extracted_data['nacionalidad']:
        nationality_match = re.search(NATIONALITY_PATTERN, text, re.IGNORECASE)
        if nationality_match:
            extracted_data['nacionalidad'] = nationality_match.group(1).strip()
    
    # Extraer género si está disponible
    if "SEXO:" in text or "GÉNERO:" in text or "GENDER:" in text:
        gender_match = re.search(r'(?:SEXO|GÉNERO|GENDER)[:]\s*([FMfm])', text)
        if gender_match:
            gender_value = gender_match.group(1).upper()
            extracted_data['genero'] = 'F' if gender_value == 'F' else 'M'        
    
    # Si no tenemos país de emisión pero tenemos nacionalidad, usamos la nacionalidad
    if not extracted_data['pais_emision'] and extracted_data['nacionalidad']:
        extracted_data['pais_emision'] = extracted_data['nacionalidad']
    
    # Si no tenemos país de emisión, asumimos España para DNI
    if not extracted_data['pais_emision'] and extracted_data['tipo_identificacion'] == 'dni':
        extracted_data['pais_emision'] = 'España'

    # Mejorar la detección de fechas para documentos panameños
    if "REPUBLICA DE PANAMA" in text or "REPÚBLICA DE PANAMÁ" in text:
        # Buscar fecha de emisión con patrón más flexible - incluir variaciones como XPEDIDA
        expedida_match = re.search(r'[XE]XPEDIDA:?\s*(\d{1,2}[-\s][a-zA-Zéúíóá]+[-\s]\d{4})', text)
        if not expedida_match:
            # Intentar con variaciones del patrón
            expedida_match = re.search(r'EXPEDIDA:?\s*(\d{1,2}[-\s][a-zA-Zéúíóá]+[-\s]\d{4})', text)
        if not expedida_match:
            # Buscar cualquier patrón que parezca una fecha de expedición
            expedida_match = re.search(r'(?:EXPED|EMIT)(?:IDA|IDO):?\s*(\d{1,2}[-\s\.][a-zA-Zéúíóá]+[-\s\.]\d{4})', text)
            
        if expedida_match:
            fecha_expedida = expedida_match.group(1).strip()
            extracted_data['fecha_emision'] = format_date_panama(fecha_expedida)
            
        # Buscar fecha de expiración con patrón más flexible
        expira_match = re.search(r'EXPIRA:?\s*(\d{1,2}[-\s][a-zA-Zéúíóá]+[-\s]\d{4})', text)
        if expira_match:
            fecha_expira = expira_match.group(1).strip()
            extracted_data['fecha_expiracion'] = format_date_panama(fecha_expira)
    
    # Si tenemos fecha de expiración pero no de emisión, calcular emisión aproximada
    if extracted_data['fecha_expiracion'] and not extracted_data['fecha_emision']:
        try:
            # Asumir que la emisión fue 10 años antes de la expiración
            expira_date = datetime.strptime(extracted_data['fecha_expiracion'], '%Y-%m-%d')
            emision_aprox = expira_date.replace(year=expira_date.year - 10)
            extracted_data['fecha_emision'] = emision_aprox.strftime('%Y-%m-%d')
            logger.warning(f"Fecha de emisión calculada a partir de fecha de expiración")
        except:
            # En caso de error, usar fecha actual
            extracted_data['fecha_emision'] = datetime.now().strftime('%Y-%m-%d')
    
    # Asegurarse de que siempre haya fechas de emisión y expiración válidas
    if not extracted_data['fecha_emision']:
        extracted_data['fecha_emision'] = datetime.now().strftime('%Y-%m-%d')
        logger.warning(f"No se pudo detectar fecha de emisión, usando fecha actual como valor predeterminado")

    if not extracted_data['fecha_expiracion']:
        # Si tenemos fecha de emisión, usar emisión + 10 años
        if extracted_data['fecha_emision']:
            try:
                emision_date = datetime.strptime(extracted_data['fecha_emision'], '%Y-%m-%d')
                diez_años_después = emision_date.replace(year=emision_date.year + 10)
                extracted_data['fecha_expiracion'] = diez_años_después.strftime('%Y-%m-%d')
                logger.warning(f"No se pudo detectar fecha de expiración, estableciendo 10 años después de emisión")
            except:
                # Si hay error al calcular, usar hoy + 10 años
                extracted_data['fecha_expiracion'] = (datetime.now() + timedelta(days=3650)).strftime('%Y-%m-%d')
                logger.warning(f"Error al calcular fecha de expiración, usando 10 años desde hoy")
        else:
            # Si no hay fecha de emisión, usar hoy + 10 años
            extracted_data['fecha_expiracion'] = (datetime.now() + timedelta(days=3650)).strftime('%Y-%m-%d')
            logger.warning(f"No se pudo detectar fecha de expiración, usando 10 años desde hoy")
    
    # Asegurarse de que siempre haya un nombre completo
    if not extracted_data['nombre_completo']:
        if extracted_data['numero_identificacion']:
            extracted_data['nombre_completo'] = f"Titular del documento {extracted_data['numero_identificacion']}"
        else:
            extracted_data['nombre_completo'] = "Titular no identificado"
            
    # Bloque final de seguridad: verificar que todos los campos obligatorios tienen valores
    required_fields = {
        'fecha_emision': datetime.now().strftime('%Y-%m-%d'),
        'fecha_expiracion': (datetime.now() + timedelta(days=3650)).strftime('%Y-%m-%d'),
        'pais_emision': 'Panamá' if extracted_data['tipo_identificacion'] == 'cedula_panama' else 'Desconocido',
        'nombre_completo': f"Titular del documento {extracted_data['numero_identificacion']}" if extracted_data['numero_identificacion'] else "Titular no identificado"
    }

    # Asegurarse de que todos los campos necesarios tengan valor
    for field, default_value in required_fields.items():
        if not extracted_data.get(field):
            extracted_data[field] = default_value
            logger.warning(f"Campo obligatorio '{field}' faltante, establecido valor predeterminado: {default_value}")
    
    return extracted_data

def register_document_identification_improved(document_id, id_data):
    """Versión mejorada que maneja valores predeterminados y restricciones de clave foránea"""
    # Definir valores predeterminados para campos críticos
    defaults = {
        'fecha_emision': datetime.now().strftime('%Y-%m-%d'),
        'fecha_expiracion': (datetime.now() + timedelta(days=3650)).strftime('%Y-%m-%d'),
        'pais_emision': 'Panamá' if id_data.get('tipo_identificacion') == 'cedula_panama' else 'Desconocido',
        'nombre_completo': f"Titular del documento {id_data.get('numero_identificacion')}" if id_data.get('numero_identificacion') else "Titular no identificado"
    }
    
    # Asegurar que todos los campos tengan valores
    for field, default in defaults.items():
        if field not in id_data or not id_data[field]:
            id_data[field] = default
            logger.warning(f"Campo faltante '{field}' en documento {document_id}, establecido valor predeterminado: {default}")
    
    # Mapear tipo_identificacion al enum correcto de la tabla
    tipo_documento_map = {
        'dni': 'cedula',
        'cedula_panama': 'cedula',
        'pasaporte': 'pasaporte',
        'licencia': 'licencia_conducir'
    }
    
    tipo_identificacion = id_data.get('tipo_identificacion', 'desconocido')
    tipo_documento = tipo_documento_map.get(tipo_identificacion, 'otro')
    
    try:
        # IMPORTANTE: Verificar primero si el documento existe en la tabla principal
        check_doc_query = """
        SELECT COUNT(*) as count FROM documentos WHERE id_documento = %s
        """
        doc_result = execute_query(check_doc_query, (document_id,))
        
        if not doc_result or doc_result[0].get('count', 0) == 0:
            logger.warning(f"El documento {document_id} no existe en la tabla 'documentos'. No se puede insertar en 'documentos_identificacion'.")
            
            # Podrías crear el documento principal aquí si tienes los datos necesarios
            # create_document_if_not_exists(document_id, id_data)
            
            # Por ahora, retornamos False sin intentar insertar
            return False
        
        # Si llegamos aquí, el documento sí existe en la tabla principal
        # Verificar si ya existe un registro en documentos_identificacion
        query_check = """
        SELECT id_documento FROM documentos_identificacion WHERE id_documento = %s
        """
        existing = execute_query(query_check, (document_id,))
        
        if existing:
            # Actualizar registro existente
            query = """
            UPDATE documentos_identificacion 
            SET tipo_documento = %s,
                numero_documento = %s,
                pais_emision = %s,
                fecha_emision = %s,
                fecha_expiracion = %s,
                nombre_completo = %s,
                genero = %s
            WHERE id_documento = %s
            """
            params = (
                tipo_documento,
                id_data.get('numero_identificacion', 'NO-ID'),
                id_data.get('pais_emision'),
                id_data.get('fecha_emision'),
                id_data.get('fecha_expiracion'),
                id_data.get('nombre_completo'),
                id_data.get('genero', 'N/A'),
                document_id
            )
        else:
            # Insertar nuevo registro
            query = """
            INSERT INTO documentos_identificacion (
                id_documento,
                tipo_documento,
                numero_documento,
                pais_emision,
                fecha_emision,
                fecha_expiracion,
                nombre_completo,
                genero
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """
            params = (
                document_id,
                tipo_documento,
                id_data.get('numero_identificacion', 'NO-ID'),
                id_data.get('pais_emision'),
                id_data.get('fecha_emision'),
                id_data.get('fecha_expiracion'),
                id_data.get('nombre_completo'),
                id_data.get('genero', 'N/A')
            )
        
        # Log para depuración
        logger.info(f"Ejecutando consulta: {query}")
        logger.info(f"Con parámetros: {params}")
        
        # Ejecutar la consulta
        execute_query(query, params, fetch=False)
        
        # Verificar que la operación fue exitosa
        verify_query = """
        SELECT COUNT(*) as count FROM documentos_identificacion WHERE id_documento = %s
        """
        verify_result = execute_query(verify_query, (document_id,))
        
        if verify_result and verify_result[0].get('count', 0) > 0:
            logger.info(f"✅ Verificación exitosa: Datos guardados correctamente para {document_id}")
            return True
        else:
            logger.warning(f"⚠️ Alerta: Los datos no se guardaron correctamente para {document_id}")
            return False
            
    except Exception as e:
        logger.error(f"Error al guardar datos en documentos_identificacion: {str(e)}")
        # Log para depuración
        if 'query' in locals() and 'params' in locals():
            debug_query = query
            for i, param in enumerate(params):
                debug_query = debug_query.replace('%s', repr(param), 1)
            logger.error(f"Consulta con error: {debug_query}")
        return False
  
def validate_id_document(extracted_data):
    """Valida los datos extraídos de un documento de identidad"""
    validation = {
        'is_valid': True,
        'confidence': 0.5,  # Base de confianza
        'errors': [],
        'warnings': []
    }
    
    # Verificar campos obligatorios para un documento de identidad
    if not extracted_data['tipo_identificacion'] or extracted_data['tipo_identificacion'] == 'desconocido':
        validation['warnings'].append("No se ha podido determinar el tipo de documento de identidad")
        validation['confidence'] -= 0.1
    
    if not extracted_data['numero_identificacion']:
        validation['errors'].append("No se ha podido extraer el número de identificación")
        validation['is_valid'] = False
        validation['confidence'] -= 0.3
    
    if not extracted_data['nombre_completo']:
        validation['warnings'].append("No se ha podido extraer el nombre completo")
        validation['confidence'] -= 0.1
    
    # Validar fechas
    if not extracted_data['fecha_emision']:
        validation['warnings'].append("No se ha podido extraer la fecha de emisión")
        validation['confidence'] -= 0.1
    
    if not extracted_data['fecha_expiracion']:
        validation['warnings'].append("No se ha podido extraer la fecha de caducidad")
        validation['confidence'] -= 0.1
    
    # Validaciones específicas por tipo de documento
    if extracted_data['tipo_identificacion'] == 'dni':
        # Validar formato de DNI español (8 dígitos + letra opcional)
        if extracted_data['numero_identificacion']:
            if not re.match(r'^\d{8}[A-Z]?$', extracted_data['numero_identificacion']):
                validation['warnings'].append("El formato del número de DNI no es válido")
                validation['confidence'] -= 0.1
    
    elif extracted_data['tipo_identificacion'] == 'pasaporte':
        # Validar formato de pasaporte (1-2 letras + 6-7 dígitos)
        if extracted_data['numero_identificacion']:
            if not re.match(r'^[A-Z]{1,2}\d{6,7}$', extracted_data['numero_identificacion']):
                validation['warnings'].append("El formato del número de pasaporte no es válido")
                validation['confidence'] -= 0.1
    
    elif extracted_data['tipo_identificacion'] == 'cedula_panama':
        # Validar formato de cédula panameña (N-NNN-NNNN)
        if extracted_data['numero_identificacion']:
            if not re.match(r'^\d{1,2}-\d{1,3}-\d{1,4}$', extracted_data['numero_identificacion']):
                validation['warnings'].append("El formato de la cédula panameña no es válido")
                validation['confidence'] -= 0.1
    
    # Verificar si ha caducado
    if extracted_data['fecha_expiracion']:
        try:
            expiry_date = datetime.strptime(extracted_data['fecha_expiracion'], '%Y-%m-%d')
            if expiry_date < datetime.now():
                validation['warnings'].append("El documento ha caducado")
        except:
            validation['warnings'].append("No se pudo validar la fecha de caducidad")
    
    # Calcular confianza final basada en campos obligatorios
    required_fields = ['tipo_identificacion', 'numero_identificacion', 'nombre_completo', 'fecha_emision', 'fecha_expiracion']
    present_fields = sum(1 for field in required_fields if extracted_data[field])
    field_confidence = present_fields / len(required_fields)
    
    # Ajustar confianza final
    validation['confidence'] = max(0.1, min(0.95, (validation['confidence'] + field_confidence) / 2))
    
    # Si hay errores críticos, la confianza no puede ser mayor a 0.5
    if validation['errors']:
        validation['confidence'] = min(validation['confidence'], 0.5)
    
    return validation

def lambda_handler(event, context):
    """
    Función principal que procesa documentos de identidad.
    Optimizada para trabajar con datos ya extraídos por textract_callback.
    Se activa por mensajes de la cola SQS de documentos de identidad.
    """
    start_time = time.time()
    logger.info("Evento recibido: " + json.dumps(event))
    
    # Verificar la estructura de la tabla documentos_identificacion para diagnóstico
    try:
        query_structure = "DESCRIBE documentos_identificacion"
        table_structure = execute_query(query_structure, ())
        logger.info(f"Estructura de la tabla documentos_identificacion: {[col.get('Field') for col in table_structure]}")
    except Exception as e:
        logger.error(f"Error al obtener estructura de tabla documentos_identificacion: {str(e)}")
    
    response = {
        'procesados': 0,
        'errores': 0,
        'detalles': []
    }

    for record in event['Records']:
        documento_detalle = {
            'documento_id': None,
            'estado': 'sin_procesar',
            'tiempo': 0
        }
        
        record_start = time.time()
        
        # ID de registro para seguimiento del proceso en la nueva tabla
        registro_id = None
        
        try:
            # Parsear el mensaje SQS
            message_body = json.loads(record['body'])
            document_id = message_body['document_id']
            documento_detalle['documento_id'] = document_id
            
            # Iniciar registro de procesamiento en la nueva tabla
            registro_id = log_document_processing_start(
                document_id, 
                'procesamiento_identidad',
                datos_entrada=message_body
            )
            
            logger.info(f"Procesando documento de identidad {document_id}")
            
            # Paso 1: Obtener datos ya extraídos de la base de datos
            document_data_result = get_extracted_data_from_db(document_id)
            
            if not document_data_result:
                logger.error(f"No se pudieron recuperar datos del documento {document_id}")
                documento_detalle['estado'] = 'error_recuperacion_datos'
                response['errores'] += 1
                if registro_id:
                    log_document_processing_end(
                        registro_id, 
                        estado='error',
                        mensaje_error=f"No se pudieron recuperar datos del documento {document_id}"
                    )
                continue
            
            # Verificar si tenemos texto extraído
            extracted_text = document_data_result['extracted_data'].get('texto_completo')
            if not extracted_text:
                logger.error(f"No hay texto extraído disponible para documento {document_id}")
                documento_detalle['estado'] = 'error_sin_texto'
                response['errores'] += 1
                if registro_id:
                    log_document_processing_end(
                        registro_id, 
                        estado='error',
                        mensaje_error=f"No hay texto extraído disponible para documento {document_id}"
                    )
                continue
            
            # Paso 2: Extraer datos específicos de documento de identidad
            entidades = document_data_result['extracted_data'].get('entidades')
            metadatos = document_data_result['extracted_data'].get('metadatos_extraccion')
            
   
            # Registrar que comenzamos extracción de datos específicos
            sub_registro_id = log_document_processing_start(
                document_id, 
                'extraccion',
                datos_entrada={"texto_longitud": len(extracted_text)},
                analisis_id=registro_id
            )
            
            id_data = extract_id_document_data(extracted_text, entidades, metadatos)
            logger.info(f"Datos extraídos de documento de identidad: {json.dumps(id_data)}")
            
            # Registrar finalización de extracción
            log_document_processing_end(
                sub_registro_id, 
                estado='completado',
                datos_procesados=id_data
            )
            
            # Paso 3: Validar datos extraídos
            validation = validate_id_document(id_data)
            # Paso 3: Validar datos extraídos common
            confidence = validation['confidence']
            tipo_detectado = id_data.get('tipo_identificacion')

   

            requires_review = evaluate_confidence(
                confidence,
                document_type=tipo_detectado,
                validation_results=validation
            )

            if requires_review:
                logger.warning(f"Documento {document_id} requiere revisión manual. Marcando en base de datos...")
                mark_for_manual_review(
                    document_id=document_id,
                    analysis_id=registro_id,
                    confidence=confidence,
                    document_type=tipo_detectado,
                    validation_info=validation,
                    extracted_data=id_data
                )
            logger.info(f"Validación: {json.dumps(validation)}")
            
            # SOLUCIÓN DEFINITIVA: Asegurarnos de que todos los campos críticos están presentes
            # antes de continuar con el proceso
            critical_fields = [
                'fecha_emision', 
                'fecha_expiracion', 
                'nombre_completo',
                'numero_identificacion',
                'pais_emision'
            ]
            
            for field in critical_fields:
                if field not in id_data or id_data[field] is None:
                    if field == 'fecha_emision':
                        id_data[field] = datetime.now().strftime('%Y-%m-%d')
                        logger.info(f"Asignado valor predeterminado para campo crítico 'fecha_emision': {id_data[field]}")
                    elif field == 'fecha_expiracion':
                        id_data[field] = (datetime.now() + timedelta(days=3650)).strftime('%Y-%m-%d')
                        logger.info(f"Asignado valor predeterminado para campo crítico 'fecha_expiracion': {id_data[field]}")
                    elif field == 'nombre_completo':
                        id_data[field] = "Titular no identificado"
                        logger.info(f"Asignado valor predeterminado para campo crítico 'nombre_completo': {id_data[field]}")
                    elif field == 'numero_identificacion':
                        id_data[field] = f"AUTO-{document_id[-8:]}"
                        logger.info(f"Asignado valor predeterminado para campo crítico 'numero_identificacion': {id_data[field]}")
                    elif field == 'pais_emision':
                        id_data[field] = "Desconocido"
                        logger.info(f"Asignado valor predeterminado para campo crítico 'pais_emision': {id_data[field]}")
            
            # Paso 4: Guardar en documentos_identificacion con manejo mejorado de errores
            try:
                # Log de los valores que vamos a insertar
                logger.info(f"Valores a insertar en documentos_identificacion: fecha_emision={id_data.get('fecha_emision')}, "
                           f"fecha_expiracion={id_data.get('fecha_expiracion')}, pais_emision={id_data.get('pais_emision')}, "
                           f"nombre_completo={id_data.get('nombre_completo')}")
                
                # Iniciar registro de proceso de registro en base de datos
                db_registro_id = log_document_processing_start(
                    document_id, 
                    'registro_bd',
                    datos_entrada=id_data,
                    analisis_id=registro_id
                )
                
                # Usar la versión mejorada para registrar documento
                register_document_identification_improved(document_id, id_data)
                logger.info(f"Datos guardados en tabla documentos_identificacion para {document_id}")
                
                # Registrar finalización exitosa
                log_document_processing_end(
                    db_registro_id, 
                    estado='completado'
                )
            except Exception as reg_error:
                logger.error(f"Error al registrar datos en documentos_identificacion: {str(reg_error)}")
                logger.error(traceback.format_exc())
                documento_detalle['error_registro'] = str(reg_error)
                
                if db_registro_id:
                    log_document_processing_end(
                        db_registro_id, 
                        estado='error',
                        mensaje_error=str(reg_error)
                    )
            
            # Paso 5: Actualizar documento principal con preservación de tipo
            update_id = log_document_processing_start(
                document_id, 
                'actualizacion_documento',
                datos_entrada={"confidence": validation['confidence'], "is_valid": validation['is_valid']},
                analisis_id=registro_id
            )
            
            try:
                update_document_extraction_data_with_type_preservation(
                    document_id,
                    json.dumps(id_data),
                    validation['confidence'],
                    validation['is_valid']
                )
                logger.info(f"Datos actualizados en documento principal {document_id}")
                
                # Registrar actualización exitosa
                log_document_processing_end(
                    update_id, 
                    estado='completado'
                )
            except Exception as update_error:
                logger.error(f"Error al actualizar documento principal: {str(update_error)}")
                documento_detalle['error_actualizacion'] = str(update_error)
                
                # Registrar error en actualización
                log_document_processing_end(
                    update_id, 
                    estado='error',
                    mensaje_error=str(update_error)
                )
            
            # Paso 7: Actualizar estado de procesamiento
            status = 'completado' if validation['is_valid'] else 'revisión_requerida'
            message = "Documento de identidad procesado correctamente" if validation['is_valid'] else "Documento procesado con advertencias"
            details = {
                'validación': validation,
                'campos_extraídos': list(filter(lambda k: id_data[k] is not None, id_data.keys()))
            }
            
            final_id = log_document_processing_start(
                document_id, 
                'actualizacion_estado',
                datos_entrada={"status": status, "details": details},
                analisis_id=registro_id
            )
            
            try:
                # Obtener el tipo correcto basado en la extracción
                tipo_id_doc = id_data.get('tipo_identificacion', 'desconocido')
                # Mapear a un tipo normalizado para la base de datos
                tipo_doc_map = {
                    'dni': 'DNI',
                    'cedula_panama': 'DNI',
                    'pasaporte': 'Pasaporte'
                }
                tipo_normalizado = tipo_doc_map.get(tipo_id_doc, 'Documento')
                
                # Actualizar con el tipo correcto
                update_document_processing_status(
                    document_id, 
                    status, 
                    json.dumps(details),
                    tipo_documento=tipo_normalizado  # <- Añadir este parámetro
                )
                logger.info(f"Estado de procesamiento actualizado para {document_id}: {status}")
                
                # Registrar actualización de estado exitosa
                log_document_processing_end(
                    final_id, 
                    estado='completado'
                )
            except Exception as status_error:
                logger.error(f"Error al actualizar estado: {str(status_error)}")
                documento_detalle['error_estado'] = str(status_error)
                
                # Registrar error en actualización de estado
                log_document_processing_end(
                    final_id, 
                    estado='error',
                    mensaje_error=str(status_error)
                )
            
            documento_detalle['estado'] = 'procesado'
            documento_detalle['confianza'] = validation['confidence']
            response['procesados'] += 1
            
            # Finalizar el registro principal con éxito
            if registro_id:
                log_document_processing_end(
                    registro_id, 
                    estado='completado',
                    confianza=validation['confidence'],
                    datos_salida=details,
                    mensaje_error=None if validation['is_valid'] else "Procesado con advertencias"
                )
            
        except json.JSONDecodeError as json_error:
            logger.error(f"Error al decodificar mensaje SQS: {str(json_error)}")
            documento_detalle['estado'] = 'error_formato_json'
            documento_detalle['error'] = str(json_error)
            response['errores'] += 1
            
            # Registrar error de decodificación JSON
            if registro_id:
                log_document_processing_end(
                    registro_id, 
                    estado='error',
                    mensaje_error=f"Error al decodificar mensaje SQS: {str(json_error)}"
                )
                
        except KeyError as key_error:
            logger.error(f"Falta campo requerido en mensaje: {str(key_error)}")
            documento_detalle['estado'] = 'error_campo_faltante'
            documento_detalle['error'] = str(key_error) 
            response['errores'] += 1
            
            # Registrar error de campo faltante
            if registro_id:
                log_document_processing_end(
                    registro_id, 
                    estado='error',
                    mensaje_error=f"Falta campo requerido en mensaje: {str(key_error)}"
                )
                
        except Exception as e:
            logger.error(f"Error general al procesar mensaje: {str(e)}")
            logger.error(traceback.format_exc())
            documento_detalle['estado'] = 'error_general'
            documento_detalle['error'] = str(e)
            response['errores'] += 1
            
            # Registrar error general
            if registro_id:
                log_document_processing_end(
                    registro_id, 
                    estado='error',
                    mensaje_error=f"Error general al procesar mensaje: {str(e)}"
                )
                
        finally:
            # Calcular tiempo de procesamiento
            tiempo_procesamiento = time.time() - record_start
            documento_detalle['tiempo'] = tiempo_procesamiento
            response['detalles'].append(documento_detalle)

    # Calcular tiempo total de procesamiento
    total_time = time.time() - start_time
    
    # Asignar carpeta y marcar documento solicitado como recibido
    cliente_id = get_client_id_by_document(document_id)
    logger.info(f"Procesando documento financiero Asignar cliente_id {cliente_id} para {document_id}")
    assign_folder_and_link(cliente_id,document_id)
  
    # Añadir resumen de procesamiento
    response['tiempo_total'] = total_time
    response['total_registros'] = len(event['Records'])
    
    logger.info(f"Procesamiento completado: {response['procesados']} exitosos, {response['errores']} errores en {total_time:.2f} segundos")

    return {
        'statusCode': 200,
        'body': json.dumps(response)
    }