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
    update_document_processing_status,
    get_document_by_id,
    log_document_processing_start,
    log_document_processing_end,
    preserve_identification_data,
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
    VERSIÓN MEJORADA: Extrae información específica de documentos de identidad
    """
    
    logger.info(f"📖 Iniciando extracción de datos de identificación")
    logger.info(f"📏 Longitud del texto: {len(text)} caracteres")
    
    # Usar la función mejorada
    extracted_data = extract_id_document_data_improved(text, entidades, metadatos)
    
    # ==================== VALORES POR DEFECTO INTELIGENTES ====================
    
    # Solo aplicar valores por defecto si realmente no se encontró nada
    if not extracted_data.get('numero_identificacion'):
        logger.warning("⚠️ No se pudo extraer número de identificación del documento")
        # NO asignar AUTO-ID aquí, mejor marcar para revisión manual
    
    if not extracted_data.get('nombre_completo'):
        logger.warning("⚠️ No se pudo extraer nombre completo del documento")
    
    if not extracted_data.get('fecha_emision'):
        logger.warning("⚠️ No se pudo extraer fecha de emisión")
        # Solo como último recurso
        extracted_data['fecha_emision'] = datetime.now().strftime('%Y-%m-%d')
    
    if not extracted_data.get('fecha_expiracion'):
        logger.warning("⚠️ No se pudo extraer fecha de expiración")
        # Calcular basado en fecha de emisión si existe
        if extracted_data.get('fecha_emision'):
            try:
                emision_date = datetime.strptime(extracted_data['fecha_emision'], '%Y-%m-%d')
                years_to_add = 10  # Por defecto 10 años
                exp_date = emision_date.replace(year=emision_date.year + years_to_add)
                extracted_data['fecha_expiracion'] = exp_date.strftime('%Y-%m-%d')
            except:
                extracted_data['fecha_expiracion'] = (datetime.now() + timedelta(days=3650)).strftime('%Y-%m-%d')
        else:
            extracted_data['fecha_expiracion'] = (datetime.now() + timedelta(days=3650)).strftime('%Y-%m-%d')
    
    # ==================== LOG DE RESULTADOS ====================
    
    tipo_detectado = extracted_data.get('tipo_identificacion', 'desconocido')
    
    if tipo_detectado != 'desconocido':
        logger.info(f"✅ Tipo de documento detectado: {tipo_detectado}")
        logger.info(f"📝 Número: {extracted_data.get('numero_identificacion', 'NO DETECTADO')}")
        logger.info(f"👤 Nombre: {extracted_data.get('nombre_completo', 'NO DETECTADO')}")
        logger.info(f"🌍 País: {extracted_data.get('pais_emision', 'NO DETECTADO')}")
        logger.info(f"📅 Emisión: {extracted_data.get('fecha_emision', 'NO DETECTADO')}")
        logger.info(f"📅 Expiración: {extracted_data.get('fecha_expiracion', 'NO DETECTADO')}")
        
        if extracted_data.get('genero'):
            logger.info(f"👥 Género: {extracted_data.get('genero')}")
        if extracted_data.get('lugar_nacimiento'):
            logger.info(f"🏠 Lugar nacimiento: {extracted_data.get('lugar_nacimiento')}")
        if extracted_data.get('autoridad_emision'):
            logger.info(f"🏛️ Autoridad: {extracted_data.get('autoridad_emision')}")
    else:
        logger.error(f"❌ No se pudo determinar el tipo de documento")
        logger.error(f"📝 Texto analizado (primeros 500 chars): {text[:500]}")
    
    return extracted_data

def extract_passport_data(text, extracted_data):
    """Extrae datos específicos de pasaportes"""
    
    # Número de pasaporte
    if not extracted_data['numero_identificacion']:
        passport_patterns = [
            r'(?i)(?:PASSPORT|PASAPORTE)\s*(?:NO|N[Oº]|NUMBER|NÚMERO)[:\s]*([A-Z0-9]{6,9})',
            r'(?i)P[A-Z]{3}(\d{5,7})',  # Formato P + país + números
            r'(?i)([A-Z]{1,2}\d{6,8})',  # Formato general de pasaporte
            r'(?i)(?:NO|N[Oº])[:\s]*([A-Z0-9]{6,9})',
        ]
        
        for pattern in passport_patterns:
            match = re.search(pattern, text)
            if match:
                extracted_data['numero_identificacion'] = match.group(1).strip()
                break
    
    # Nombres y apellidos (formato pasaporte)
    if not extracted_data['nombre_completo']:
        name_patterns = [
            r'(?i)SURNAME[:\s]+([A-ZÁÉÍÓÚÑ\s]+)\s+GIVEN\s+NAMES?[:\s]+([A-ZÁÉÍÓÚÑ\s]+)',
            r'(?i)APELLIDOS?[:\s]+([A-ZÁÉÍÓÚÑ\s]+)\s+NOMBRES?[:\s]+([A-ZÁÉÍÓÚÑ\s]+)',
            r'(?i)NOM[:\s]+([A-ZÁÉÍÓÚÑ\s]+)\s+PRENOM[:\s]+([A-ZÁÉÍÓÚÑ\s]+)',
        ]
        
        for pattern in name_patterns:
            match = re.search(pattern, text)
            if match:
                apellidos = match.group(1).strip()
                nombres = match.group(2).strip()
                extracted_data['apellidos'] = apellidos
                extracted_data['nombre'] = nombres
                extracted_data['nombre_completo'] = f"{nombres} {apellidos}"
                break
    
    # Lugar de nacimiento
    birth_place_patterns = [
        r'(?i)(?:PLACE OF BIRTH|LUGAR DE NACIMIENTO)[:\s]+([A-ZÁÉÍÓÚÑ\s,]+)',
        r'(?i)(?:LIEU DE NAISSANCE)[:\s]+([A-ZÁÉÍÓÚÑ\s,]+)',
    ]
    
    for pattern in birth_place_patterns:
        match = re.search(pattern, text)
        if match:
            extracted_data['lugar_nacimiento'] = match.group(1).strip()
            break
    
    # Autoridad de emisión
    authority_patterns = [
        r'(?i)(?:AUTHORITY|AUTORIDAD)[:\s]+([A-ZÁÉÍÓÚÑ\s,]+)',
        r'(?i)(?:ISSUED BY|EMITIDO POR)[:\s]+([A-ZÁÉÍÓÚÑ\s,]+)',
    ]
    
    for pattern in authority_patterns:
        match = re.search(pattern, text)
        if match:
            extracted_data['autoridad_emision'] = match.group(1).strip()
            break
    
    # Fechas (formato internacional)
    if not extracted_data['fecha_emision']:
        issue_patterns = [
            r'(?i)(?:DATE OF ISSUE|FECHA DE EXPEDICIÓN)[:\s]+(\d{1,2}[/.\s]\d{1,2}[/.\s]\d{2,4})',
            r'(?i)(?:DATE D\'ÉMISSION)[:\s]+(\d{1,2}[/.\s]\d{1,2}[/.\s]\d{2,4})',
        ]
        
        for pattern in issue_patterns:
            match = re.search(pattern, text)
            if match:
                extracted_data['fecha_emision'] = normalize_date_international(match.group(1))
                break
    
    if not extracted_data['fecha_expiracion']:
        expiry_patterns = [
            r'(?i)(?:DATE OF EXPIRY|FECHA DE CADUCIDAD)[:\s]+(\d{1,2}[/.\s]\d{1,2}[/.\s]\d{2,4})',
            r'(?i)(?:DATE D\'EXPIRATION)[:\s]+(\d{1,2}[/.\s]\d{1,2}[/.\s]\d{2,4})',
        ]
        
        for pattern in expiry_patterns:
            match = re.search(pattern, text)
            if match:
                extracted_data['fecha_expiracion'] = normalize_date_international(match.group(1))
                break
    
    # Sexo/Género
    if not extracted_data['genero']:
        sex_patterns = [
            r'(?i)(?:SEX|SEXO|SEXE)[:\s]+([MFmf])',
        ]
        
        for pattern in sex_patterns:
            match = re.search(pattern, text)
            if match:
                extracted_data['genero'] = match.group(1).upper()
                break
    
    # Nacionalidad
    if not extracted_data['nacionalidad']:
        nationality_patterns = [
            r'(?i)(?:NATIONALITY|NACIONALIDAD|NATIONALITÉ)[:\s]+([A-ZÁÉÍÓÚÑ\s]+)',
        ]
        
        for pattern in nationality_patterns:
            match = re.search(pattern, text)
            if match:
                extracted_data['nacionalidad'] = match.group(1).strip()
                break
    
    return extracted_data

def extract_cedula_panama_data(text, extracted_data):
    """Extrae datos específicos de cédulas panameñas (mantiene tu lógica actual)"""
    
    # Tu lógica actual para cédulas panameñas
    if "REPUBLICA DE PANAMA" in text or "REPÚBLICA DE PANAMÁ" in text:
        extracted_data['pais_emision'] = 'Panamá'
        
        # Extraer número de cédula si no se ha hecho ya
        if not extracted_data['numero_identificacion']:
            cedula_match = re.search(r'\b(\d{1,2}-\d{3,4}-\d{1,4})\b', text)
            if cedula_match:
                extracted_data['numero_identificacion'] = cedula_match.group(1)
                extracted_data['tipo_identificacion'] = 'cedula_panama'
        
        # Extraer nombre completo con patrones específicos para documentos panameños
        if not extracted_data['nombre_completo']:
            # Tus patrones actuales...
            nombre_patterns = [
                r'IDENTIDAD\s+\d+\s+([A-ZÁÉÍÓÚÑ\s]+)\s+NOMBRE\s+USUAL',
                r'TRIBUNAL\s+ELECTORAL\s+([A-Za-záéíóúñÁÉÍÓÚÑ\s]+)\s+NOMBRE\s+USUAL',
                r'ELECTORAL\s+([A-Za-záéíóúñÁÉÍÓÚÑ\s]+)\s+(?:F|E)ECHA\s+DE\s+NACIMIENTO',
                r'P\s+A\s+([\w\s]+)\s+N\s+A\s+([\w\s]+)\s+M\s+\d+\s+A',
            ]
            
            for pattern in nombre_patterns:
                match = re.search(pattern, text)
                if match:
                    if len(match.groups()) == 1:
                        extracted_data['nombre_completo'] = match.group(1).strip()
                    else:
                        nombre = match.group(1).strip()
                        apellido = match.group(2).strip()
                        extracted_data['nombre_completo'] = f"{nombre} {apellido}"
                        extracted_data['nombre'] = nombre
                        extracted_data['apellidos'] = apellido
                    break
        
        # Fechas específicas para Panamá
        if not extracted_data['fecha_emision']:
            expedida_patterns = [
                r'[XE]XPEDIDA:?\s*(\d{1,2}[-\s][a-zA-Zéúíóá]+[-\s]\d{4})',
                r'EXPEDIDA:?\s*(\d{1,2}[-\s][a-zA-Zéúíóá]+[-\s]\d{4})',
            ]
            
            for pattern in expedida_patterns:
                match = re.search(pattern, text)
                if match:
                    extracted_data['fecha_emision'] = format_date_panama(match.group(1).strip())
                    break
        
        if not extracted_data['fecha_expiracion']:
            expira_match = re.search(r'EXPIRA:?\s*(\d{1,2}[-\s][a-zA-Zéúíóá]+[-\s]\d{4})', text)
            if expira_match:
                extracted_data['fecha_expiracion'] = format_date_panama(expira_match.group(1).strip())
    
    return extracted_data

def extract_dni_spain_data(text, extracted_data):
    """Extrae datos específicos de DNI español"""
    
    extracted_data['pais_emision'] = 'España'
    
    # Número de DNI
    if not extracted_data['numero_identificacion']:
        dni_patterns = [
            r'(?i)DNI[:\s]*(\d{8}[A-Z])',
            r'(?i)DOCUMENTO[:\s]+(\d{8}[A-Z])',
            r'\b(\d{8}[A-Z])\b',
        ]
        
        for pattern in dni_patterns:
            match = re.search(pattern, text)
            if match:
                extracted_data['numero_identificacion'] = match.group(1)
                break
    
    # Resto de la extracción similar a la actual...
    
    return extracted_data

def extract_generic_id_data(text, extracted_data):
    """Extracción genérica para documentos no identificados específicamente"""
    
    # Patrones genéricos para cualquier documento de identidad
    if not extracted_data['numero_identificacion']:
        generic_patterns = [
            r'(?i)(?:ID|IDENTIFICATION)[:\s]*([A-Z0-9]{5,15})',
            r'(?i)(?:NÚMERO|NUMBER)[:\s]*([A-Z0-9]{5,15})',
            r'(?i)(?:DOC|DOCUMENTO)[:\s]*([A-Z0-9]{5,15})',
        ]
        
        for pattern in generic_patterns:
            match = re.search(pattern, text)
            if match:
                extracted_data['numero_identificacion'] = match.group(1)
                break
    
    return extracted_data

def normalize_date_international(date_str):
    """Normaliza fechas en formatos internacionales"""
    if not date_str:
        return None
    
    try:
        # Limpiar la fecha
        clean_date = re.sub(r'[^\d/.\s]', '', date_str)
        
        # Intentar diferentes formatos
        formats = ['%d/%m/%Y', '%m/%d/%Y', '%d.%m.%Y', '%Y-%m-%d', '%d %m %Y']
        
        for fmt in formats:
            try:
                date_obj = datetime.strptime(clean_date.strip(), fmt)
                return date_obj.strftime('%Y-%m-%d')
            except ValueError:
                continue
                
        return None
    except Exception:
        return None

def register_document_identification_improved(document_id, id_data):
    """
    Versión mejorada que NO inserta datos falsos
    """
    
    # ==================== VALIDACIÓN PREVIA ====================
    
    # Verificar que tenemos datos mínimos reales
    required_real_data = [
        'numero_identificacion',
        'nombre_completo'
    ]
    
    missing_critical = []
    for field in required_real_data:
        value = id_data.get(field)
        if not value or value.startswith('AUTO-') or value in ['Titular no identificado', 'NO-ID']:
            missing_critical.append(field)
    
    # Si faltan datos críticos, NO insertar y marcar para revisión manual
    if missing_critical:
        logger.error(f"❌ Datos críticos faltantes: {missing_critical}")
        logger.error(f"❌ NO SE INSERTARÁ en base de datos. Documento requiere revisión manual.")
        
        # Marcar documento para revisión manual en lugar de insertar datos falsos
        try:
            update_document_processing_status(
                document_id, 
                'requiere_revision_manual',
                f"Datos críticos no extraídos: {', '.join(missing_critical)}. Revisión manual necesaria."
            )
            return False
        except Exception as e:
            logger.error(f"Error al marcar para revisión manual: {str(e)}")
            return False
    
    # ==================== PREPARACIÓN DE DATOS ====================
    
    # Mapear tipo_identificacion al enum de la tabla
    tipo_documento_map = {
        'dni': 'cedula',
        'cedula_panama': 'cedula', 
        'cedula': 'cedula',
        'pasaporte': 'pasaporte',
        'licencia': 'licencia_conducir'
    }
    
    tipo_identificacion = id_data.get('tipo_identificacion', 'desconocido')
    tipo_documento = tipo_documento_map.get(tipo_identificacion, 'otro')
    
    # Generar código de país
    codigo_pais = get_country_code(id_data.get('pais_emision', ''))
    
    # ==================== VALIDACIONES ADICIONALES ====================
    
    # Validar fechas
    fecha_emision = id_data.get('fecha_emision')
    fecha_expiracion = id_data.get('fecha_expiracion')
    
    if not fecha_emision or not fecha_expiracion:
        logger.warning(f"⚠️ Fechas incompletas - Emisión: {fecha_emision}, Expiración: {fecha_expiracion}")
        # Solo usar fechas por defecto si realmente no hay datos
        if not fecha_emision:
            fecha_emision = datetime.now().strftime('%Y-%m-%d')
        if not fecha_expiracion:
            # Calcular basado en tipo de documento
            try:
                base_date = datetime.strptime(fecha_emision, '%Y-%m-%d')
                years_to_add = 10 if tipo_documento == 'pasaporte' else 10
                exp_date = base_date.replace(year=base_date.year + years_to_add)
                fecha_expiracion = exp_date.strftime('%Y-%m-%d')
            except:
                fecha_expiracion = (datetime.now() + timedelta(days=3650)).strftime('%Y-%m-%d')
    
    try:
        # ==================== VERIFICAR DOCUMENTO EXISTENTE ====================
        
        check_doc_query = """
        SELECT COUNT(*) as count FROM documentos WHERE id_documento = %s
        """
        doc_result = execute_query(check_doc_query, (document_id,))
        
        if not doc_result or doc_result[0].get('count', 0) == 0:
            logger.error(f"❌ El documento {document_id} no existe en la tabla 'documentos'")
            return False
        
        # ==================== VERIFICAR SI YA EXISTE REGISTRO ====================
        
        query_check = """
        SELECT id_documento FROM documentos_identificacion WHERE id_documento = %s
        """
        existing = execute_query(query_check, (document_id,))
        
        if existing:
            # PRESERVAR DATOS ANTES DE ACTUALIZAR
            logger.info(f"📸 Preservando datos existentes antes de actualizar")
            try:
                preserve_identification_data(
                    document_id, 
                    reason="Actualización con nuevos datos extraídos"
                )
            except Exception as preserve_error:
                logger.warning(f"⚠️ Error al preservar datos: {str(preserve_error)}")
            
            # ACTUALIZAR registro existente
            query = """
            UPDATE documentos_identificacion 
            SET tipo_documento = %s,
                numero_documento = %s,
                pais_emision = %s,
                fecha_emision = %s,
                fecha_expiracion = %s,
                nombre_completo = %s,
                genero = %s,
                lugar_nacimiento = %s,
                autoridad_emision = %s,
                nacionalidad = %s,
                codigo_pais = %s
            WHERE id_documento = %s
            """
            params = (
                tipo_documento,
                id_data.get('numero_identificacion'),
                id_data.get('pais_emision'),
                fecha_emision,
                fecha_expiracion,
                id_data.get('nombre_completo'),
                id_data.get('genero'),
                id_data.get('lugar_nacimiento'),
                id_data.get('autoridad_emision'),
                id_data.get('nacionalidad'),
                codigo_pais,
                document_id
            )
            operation = "ACTUALIZACIÓN"
        else:
            # INSERTAR nuevo registro
            query = """
            INSERT INTO documentos_identificacion (
                id_documento,
                tipo_documento,
                numero_documento,
                pais_emision,
                fecha_emision,
                fecha_expiracion,
                nombre_completo,
                genero,
                lugar_nacimiento,
                autoridad_emision,
                nacionalidad,
                codigo_pais
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            params = (
                document_id,
                tipo_documento,
                id_data.get('numero_identificacion'),
                id_data.get('pais_emision'),
                fecha_emision,
                fecha_expiracion,
                id_data.get('nombre_completo'),
                id_data.get('genero'),
                id_data.get('lugar_nacimiento'),
                id_data.get('autoridad_emision'),
                id_data.get('nacionalidad'),
                codigo_pais
            )
            operation = "INSERCIÓN"
        
        # ==================== EJECUTAR CONSULTA ====================
        
        logger.info(f"🔍 {operation} para {tipo_documento.upper()}")
        logger.info(f"📝 Número: {id_data.get('numero_identificacion')}")
        logger.info(f"👤 Nombre: {id_data.get('nombre_completo')}")
        logger.info(f"🌍 País: {id_data.get('pais_emision')} ({codigo_pais})")
        logger.info(f"📅 Vigencia: {fecha_emision} → {fecha_expiracion}")
        
        if tipo_documento == 'pasaporte':
            logger.info(f"📔 Datos específicos de PASAPORTE:")
            if id_data.get('lugar_nacimiento'):
                logger.info(f"   🏠 Lugar nacimiento: {id_data.get('lugar_nacimiento')}")
            if id_data.get('autoridad_emision'):
                logger.info(f"   🏛️ Autoridad: {id_data.get('autoridad_emision')}")
            if id_data.get('nacionalidad'):
                logger.info(f"   🏳️ Nacionalidad: {id_data.get('nacionalidad')}")
        
        # Ejecutar la consulta
        execute_query(query, params, fetch=False)
        
        # ==================== VERIFICAR ÉXITO ====================
        
        verify_query = """
        SELECT numero_documento, nombre_completo, tipo_documento 
        FROM documentos_identificacion 
        WHERE id_documento = %s
        """
        verify_result = execute_query(verify_query, (document_id,))
        
        if verify_result and len(verify_result) > 0:
            saved_data = verify_result[0]
            logger.info(f"✅ {operation} exitosa verificada:")
            logger.info(f"   📝 Número guardado: {saved_data.get('numero_documento')}")
            logger.info(f"   👤 Nombre guardado: {saved_data.get('nombre_completo')}")
            logger.info(f"   📋 Tipo guardado: {saved_data.get('tipo_documento')}")
            return True
        else:
            logger.error(f"❌ Verificación falló: No se encontraron datos guardados")
            return False
            
    except Exception as e:
        logger.error(f"❌ Error en {operation if 'operation' in locals() else 'registro'}: {str(e)}")
        logger.error(f"📊 Datos que se intentaban guardar:")
        logger.error(f"   Tipo: {tipo_identificacion} → {tipo_documento}")
        logger.error(f"   Número: {id_data.get('numero_identificacion')}")
        logger.error(f"   Nombre: {id_data.get('nombre_completo')}")
        
        return False

def get_country_code(country_name):
    """Convierte nombre de país a código ISO de 3 letras con manejo de errores mejorado"""
    if not country_name or not isinstance(country_name, str):
        logger.debug(f"🔍 Nombre de país inválido: {country_name}")
        return None
        
    # Limpiar el nombre del país
    clean_country = country_name.strip()
    if not clean_country:
        return None
        
    country_codes = {
        # Países principales
        'Panamá': 'PAN', 'Panama': 'PAN', 'PANAMÁ': 'PAN', 'PANAMA': 'PAN',
        'España': 'ESP', 'Spain': 'ESP', 'ESPAÑA': 'ESP', 'SPAIN': 'ESP',
        
        # Américas
        'Estados Unidos': 'USA', 'United States': 'USA', 'USA': 'USA', 'US': 'USA',
        'Colombia': 'COL', 'COLOMBIA': 'COL',
        'México': 'MEX', 'Mexico': 'MEX', 'MÉXICO': 'MEX', 'MEXICO': 'MEX',
        'Argentina': 'ARG', 'ARGENTINA': 'ARG',
        'Brasil': 'BRA', 'Brazil': 'BRA', 'BRASIL': 'BRA', 'BRAZIL': 'BRA',
        'Chile': 'CHL', 'CHILE': 'CHL',
        'Perú': 'PER', 'Peru': 'PER', 'PERÚ': 'PER', 'PERU': 'PER',
        'Ecuador': 'ECU', 'ECUADOR': 'ECU',
        'Venezuela': 'VEN', 'VENEZUELA': 'VEN',
        'Costa Rica': 'CRI', 'COSTA RICA': 'CRI',
        'Guatemala': 'GTM', 'GUATEMALA': 'GTM',
        'Honduras': 'HND', 'HONDURAS': 'HND',
        'Nicaragua': 'NIC', 'NICARAGUA': 'NIC',
        'El Salvador': 'SLV', 'EL SALVADOR': 'SLV',
        'República Dominicana': 'DOM', 'REPÚBLICA DOMINICANA': 'DOM',
        'Cuba': 'CUB', 'CUBA': 'CUB',
        'Jamaica': 'JAM', 'JAMAICA': 'JAM',
        'Canadá': 'CAN', 'Canada': 'CAN', 'CANADÁ': 'CAN', 'CANADA': 'CAN',
        
        # Europa
        'Francia': 'FRA', 'France': 'FRA', 'FRANCIA': 'FRA', 'FRANCE': 'FRA',
        'Reino Unido': 'GBR', 'United Kingdom': 'GBR', 'REINO UNIDO': 'GBR', 'UK': 'GBR',
        'Italia': 'ITA', 'Italy': 'ITA', 'ITALIA': 'ITA', 'ITALY': 'ITA',
        'Alemania': 'DEU', 'Germany': 'DEU', 'ALEMANIA': 'DEU', 'GERMANY': 'DEU',
        'Portugal': 'PRT', 'PORTUGAL': 'PRT',
        'Países Bajos': 'NLD', 'Netherlands': 'NLD', 'PAÍSES BAJOS': 'NLD',
        
        # Asia
        'China': 'CHN', 'CHINA': 'CHN',
        'Japón': 'JPN', 'Japan': 'JPN', 'JAPÓN': 'JPN', 'JAPAN': 'JPN',
        'India': 'IND', 'INDIA': 'IND',
        'Corea del Sur': 'KOR', 'South Korea': 'KOR', 'COREA DEL SUR': 'KOR'
    }
    
    try:
        # Buscar coincidencia exacta primero
        if clean_country in country_codes:
            return country_codes[clean_country]
        
        # Buscar coincidencia parcial (case-insensitive)
        country_upper = clean_country.upper()
        for country, code in country_codes.items():
            if country.upper() == country_upper:
                return code
        
        # Si no se encuentra, intentar búsqueda parcial
        for country, code in country_codes.items():
            if country_upper in country.upper() or country.upper() in country_upper:
                logger.info(f"🔍 Coincidencia parcial encontrada: '{clean_country}' -> {code}")
                return code
        
        # Si no se encuentra nada, log de debug solamente
        logger.debug(f"🔍 Código de país no encontrado para: '{clean_country}'")
        return None
        
    except Exception as e:
        logger.error(f"❌ Error al procesar código de país '{clean_country}': {str(e)}")
        return None

def get_default_country(tipo_identificacion):
    """Retorna el país por defecto según el tipo de identificación"""
    defaults = {
        'cedula_panama': 'Panamá',
        'cedula': 'Panamá',  # Asumiendo que la mayoría son panameñas
        'dni': 'España',
        'pasaporte': 'Desconocido',  # Los pasaportes pueden ser de cualquier país
        'licencia': 'Panamá'
    }
    return defaults.get(tipo_identificacion, 'Desconocido')

# Añadir función para visualizar cambios después del procesamiento
def log_identification_changes(document_id):
    """
    Registra y muestra los cambios detectados en los datos de identificación
    """
    try:
        # Obtener el último registro histórico
        history_query = """
        SELECT h.*, v.numero_version
        FROM historico_documentos_identificacion h
        JOIN versiones_documento v ON h.id_version = v.id_version
        WHERE h.id_documento = %s
        ORDER BY h.fecha_preservacion DESC
        LIMIT 1
        """
        
        historical = execute_query(history_query, (document_id,))
        
        if not historical:
            logger.info("📋 No hay datos históricos para comparar")
            return
        
        # Obtener datos actuales
        current_query = """
        SELECT * FROM documentos_identificacion
        WHERE id_documento = %s
        """
        
        current = execute_query(current_query, (document_id,))
        
        if not current:
            return
        
        hist = historical[0]
        curr = current[0]
        
        # Comparar y registrar cambios
        changes = []
        
        if hist['numero_documento'] != curr['numero_documento']:
            changes.append(f"📝 Número: {hist['numero_documento']} → {curr['numero_documento']}")
        
        if hist['nombre_completo'] != curr['nombre_completo']:
            changes.append(f"👤 Nombre: {hist['nombre_completo']} → {curr['nombre_completo']}")
        
        if hist['fecha_expiracion'] != curr['fecha_expiracion']:
            changes.append(f"📅 Expiración: {hist['fecha_expiracion']} → {curr['fecha_expiracion']}")
        
        if changes:
            logger.info(f"🔄 Cambios detectados en documento {document_id}:")
            for change in changes:
                logger.info(f"   {change}")
        else:
            logger.info(f"✅ Sin cambios en los datos de identificación")
            
    except Exception as e:
        logger.error(f"Error al registrar cambios: {str(e)}")

def extract_id_document_data_improved(text, entidades=None, metadatos=None):
    """
    Versión mejorada para extraer información de documentos de identidad
    con mejores patrones y lógica de detección
    """
    
    # Limpiar texto para mejor procesamiento
    text_clean = re.sub(r'\s+', ' ', text.strip())
    text_upper = text_clean.upper()
    
    # Resultado inicial
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
        'lugar_nacimiento': None,
        'autoridad_emision': None,
        'texto_completo': text
    }
    
    # ==================== DETECCIÓN MEJORADA DEL TIPO DE DOCUMENTO ====================
    
    # Indicadores más específicos para cada tipo de documento
    passport_score = 0
    cedula_panama_score = 0
    dni_spain_score = 0
    
    # Patrones para PASAPORTE
    if re.search(r'PASAPORTE|PASSPORT', text_upper):
        passport_score += 3
    if re.search(r'REPUBLIC OF PANAMA|REPUBLICA DE PANAMA.*PASSPORT', text_upper):
        passport_score += 3
    if re.search(r'SURNAME.*GIVEN NAMES|APELLIDOS.*NOMBRES', text_upper):
        passport_score += 2
    if re.search(r'AUTORIDAD.*PASAPORTES|AUTHORITY.*PASSPORT', text_upper):
        passport_score += 2
    if re.search(r'P[A-Z]{2}\d{7}|[A-Z]{2}\d{7}', text):  # Formato número pasaporte
        passport_score += 2
    if re.search(r'DATE OF BIRTH|DATE OF ISSUE|DATE OF EXPIRY', text_upper):
        passport_score += 1
    
    # Patrones para CÉDULA PANAMEÑA
    if re.search(r'TRIBUNAL ELECTORAL', text_upper):
        cedula_panama_score += 3
    if re.search(r'REPUBLICA DE PANAMA.*TRIBUNAL', text_upper):
        cedula_panama_score += 2
    if re.search(r'\d{1,2}-\d{3,4}-\d{1,4}', text):  # Formato cédula panameña
        cedula_panama_score += 3
    if re.search(r'EXPEDIDA.*EXPIRA', text_upper):
        cedula_panama_score += 2
    if re.search(r'TIPO DE SANGRE|DONADOR', text_upper):
        cedula_panama_score += 2
    
    # Patrones para DNI ESPAÑOL
    if re.search(r'ESPAÑA|SPAIN', text_upper):
        dni_spain_score += 2
    if re.search(r'DOCUMENTO NACIONAL DE IDENTIDAD|DNI', text_upper):
        dni_spain_score += 3
    if re.search(r'\d{8}[A-Z]', text):  # Formato DNI español
        dni_spain_score += 3
    
    # Determinar tipo de documento por mayor puntuación
    if passport_score >= 3:
        extracted_data['tipo_identificacion'] = 'pasaporte'
        logger.info(f"📔 PASAPORTE detectado (score: {passport_score})")
    elif cedula_panama_score >= 3:
        extracted_data['tipo_identificacion'] = 'cedula_panama'
        logger.info(f"🆔 CÉDULA PANAMEÑA detectada (score: {cedula_panama_score})")
    elif dni_spain_score >= 3:
        extracted_data['tipo_identificacion'] = 'dni'
        logger.info(f"🪪 DNI ESPAÑOL detectado (score: {dni_spain_score})")
    
    # ==================== EXTRACCIÓN ESPECÍFICA POR TIPO ====================
    
    if extracted_data['tipo_identificacion'] == 'pasaporte':
        extracted_data = extract_passport_data_improved(text, text_upper, extracted_data)
    elif extracted_data['tipo_identificacion'] == 'cedula_panama':
        extracted_data = extract_cedula_panama_data_improved(text, text_upper, extracted_data)
    elif extracted_data['tipo_identificacion'] == 'dni':
        extracted_data = extract_dni_spain_data(text, text_upper, extracted_data)
    
    # ==================== VALIDACIÓN Y LIMPIEZA FINAL ====================
    
    # Limpiar y validar datos extraídos
    extracted_data = clean_and_validate_data(extracted_data)
    
    return extracted_data

def extract_passport_data_improved(text, text_upper, extracted_data):
    """Extracción mejorada para pasaportes"""
    
    # 1. NÚMERO DE PASAPORTE - Patrones mejorados
    passport_number_patterns = [
        r'PASSPORT\s+NO[:\s]+([A-Z]{2}\d{7})',  # PA0106480
        r'PASAPORTE\s+NO[:\s]+([A-Z]{2}\d{7})',
        r'NO[:\s]*([A-Z]{2}\d{7})',
        r'([A-Z]{2}\d{7})',  # Patrón general para números como PA0106480, PD0404102
    ]
    
    for pattern in passport_number_patterns:
        match = re.search(pattern, text_upper)
        if match:
            extracted_data['numero_identificacion'] = match.group(1)
            logger.info(f"📝 Número de pasaporte encontrado: {match.group(1)}")
            break
    
    # 2. NOMBRES Y APELLIDOS
    name_patterns = [
        # Formato: APELLIDOS/SURNAME ... NOMBRES/GIVEN NAMES ...
        r'APELLIDOS/SURNAME\s+([A-Z\s]+)\s+NOMBRES\s*/\s*GIVEN\s+NAMES\s+([A-Z\s]+?)(?:\s+SPECIMEN|\s+\d|\s+[A-Z]{3}|$)',
        r'SURNAME\s+([A-Z\s]+)\s+GIVEN\s+NAMES\s+([A-Z\s]+?)(?:\s+SPECIMEN|\s+\d|\s+[A-Z]{3}|$)',
        # Buscar en la línea que contiene nombres
        r'([A-Z]+)\s+([A-Z]+)\s+([A-Z\s]+)\s+([A-Z\s]+)',  # Apellido1 Apellido2 Nombre1 Nombre2
    ]
    
    for pattern in name_patterns:
        match = re.search(pattern, text_upper)
        if match and len(match.groups()) >= 2:
            if len(match.groups()) == 2:
                apellidos = match.group(1).strip()
                nombres = match.group(2).strip()
            else:
                # Si hay 4 grupos, asumir que son apellido1 apellido2 nombre1 nombre2
                apellidos = f"{match.group(1).strip()} {match.group(2).strip()}"
                nombres = f"{match.group(3).strip()} {match.group(4).strip()}"
            
            extracted_data['apellidos'] = apellidos
            extracted_data['nombre'] = nombres
            extracted_data['nombre_completo'] = f"{nombres} {apellidos}"
            logger.info(f"👤 Nombre completo: {extracted_data['nombre_completo']}")
            break
    
    # Si no encontramos con patrones estructurados, buscar nombres en texto
    if not extracted_data['nombre_completo']:
        # Buscar líneas que contengan nombres (entre números de pasaporte y fecha de nacimiento)
        lines = text.split('\n')
        for i, line in enumerate(lines):
            line_upper = line.upper()
            # Si la línea contiene un patrón de nombre (solo letras y espacios, al menos 2 palabras)
            if re.match(r'^[A-Z\s]{10,50}$', line_upper.strip()) and len(line_upper.strip().split()) >= 2:
                potential_name = line.strip()
                if len(potential_name) > 10:  # Evitar líneas muy cortas
                    extracted_data['nombre_completo'] = potential_name
                    logger.info(f"👤 Nombre encontrado por patrón general: {potential_name}")
                    break
    
    # 3. PAÍS DE EMISIÓN
    country_patterns = [
        r'REPUBLICA DE PANAMA|REPUBLIC OF PANAMA',
        r'ESPAÑA|SPAIN',
        r'COLOMBIA',
    ]
    
    for pattern in country_patterns:
        if re.search(pattern, text_upper):
            if 'PANAMA' in pattern:
                extracted_data['pais_emision'] = 'Panamá'
            elif 'ESPAÑA' in pattern or 'SPAIN' in pattern:
                extracted_data['pais_emision'] = 'España'
            elif 'COLOMBIA' in pattern:
                extracted_data['pais_emision'] = 'Colombia'
            break
    
    # 4. FECHAS (emisión y expiración)
    date_patterns = [
        r'DATE OF ISSUE\s+(\d{1,2}\s+[A-Z]{3}\s+\d{4})',  # 02 ENE 2014
        r'FECHA DE EXPEDICION\s+(\d{1,2}\s+[A-Z]{3}\s+\d{4})',
        r'DATE OF EXPIRY\s+(\d{1,2}\s+[A-Z]{3}\s+\d{4})',  # 02 ENE 2019
        r'FECHA DE VENCIMIENTO\s+(\d{1,2}\s+[A-Z]{3}\s+\d{4})',
    ]
    
    for pattern in date_patterns:
        matches = re.findall(pattern, text_upper)
        if matches:
            if 'ISSUE' in pattern or 'EXPEDICION' in pattern:
                extracted_data['fecha_emision'] = convert_spanish_date(matches[0])
                logger.info(f"📅 Fecha emisión: {extracted_data['fecha_emision']}")
            elif 'EXPIRY' in pattern or 'VENCIMIENTO' in pattern:
                extracted_data['fecha_expiracion'] = convert_spanish_date(matches[0])
                logger.info(f"📅 Fecha expiración: {extracted_data['fecha_expiracion']}")
    
    # 5. GÉNERO
    gender_patterns = [
        r'SEXO/SEX\s+([MF])',
        r'SEX\s+([MF])',
        r'SEXO\s+([MF])',
    ]
    
    for pattern in gender_patterns:
        match = re.search(pattern, text_upper)
        if match:
            extracted_data['genero'] = match.group(1)
            break
    
    # 6. FECHA DE NACIMIENTO
    birth_date_patterns = [
        r'DATE OF BIRTH\s+(\d{1,2}\s+[A-Z]{3}\s+\d{4})',  # 21 MAR 1991
        r'FECHA DE NACIMIENTO\s+(\d{1,2}\s+[A-Z]{3}\s+\d{4})',
        r'(\d{1,2}\s+[A-Z]{3}\s+\d{4})',  # Patrón general de fecha
    ]
    
    for pattern in birth_date_patterns:
        match = re.search(pattern, text_upper)
        if match:
            extracted_data['fecha_nacimiento'] = convert_spanish_date(match.group(1))
            logger.info(f"🎂 Fecha nacimiento: {extracted_data['fecha_nacimiento']}")
            break
    
    # 7. AUTORIDAD DE EMISIÓN
    if 'PASAPORTES/PANAMA' in text_upper:
        extracted_data['autoridad_emision'] = 'Pasaportes/Panamá'
    elif 'AUTORIDAD' in text_upper:
        auth_match = re.search(r'AUTORIDAD[:\s]+([A-Z\s/]+)', text_upper)
        if auth_match:
            extracted_data['autoridad_emision'] = auth_match.group(1).strip()
    
    # 8. NACIONALIDAD
    nationality_patterns = [
        r'NACIONALIDAD[:\s/]+([A-Z]+)',
        r'NATIONALITY[:\s/]+([A-Z]+)',
    ]
    
    for pattern in nationality_patterns:
        match = re.search(pattern, text_upper)
        if match:
            nationality = match.group(1).strip()
            if nationality == 'PANAMENA':
                extracted_data['nacionalidad'] = 'Panameña'
            elif nationality == 'ESPANOLA':
                extracted_data['nacionalidad'] = 'Española'
            else:
                extracted_data['nacionalidad'] = nationality
            break
    
    return extracted_data

def extract_cedula_panama_data_improved(text, text_upper, extracted_data):
    """Extracción mejorada para cédulas panameñas"""
    
    extracted_data['pais_emision'] = 'Panamá'
    
    # 1. NÚMERO DE CÉDULA
    cedula_patterns = [
        r'\b(\d{1,2}-\d{3,4}-\d{1,4})\b',  # 8-236-51, 8-823-2320
    ]
    
    for pattern in cedula_patterns:
        match = re.search(pattern, text)
        if match:
            extracted_data['numero_identificacion'] = match.group(1)
            logger.info(f"📝 Número de cédula: {match.group(1)}")
            break
    
    # 2. NOMBRE COMPLETO - Patrones específicos para cédulas panameñas
    name_patterns = [
        r'TRIBUNAL ELECTORAL\s+([A-Z\s]+)\s+NOMBRE USUAL',
        r'ELECTORAL\s+([A-Z\s]+)\s+NOMBRE',
        r'P\s+A\s+([A-Z\s]+)\s+N\s+A\s+([A-Z\s]+)\s+M',  # Patrón específico del ejemplo
        # Buscar nombres entre ELECTORAL y NOMBRE USUAL
        r'ELECTORAL\s+([A-Za-z\s]+?)\s+NOMBRE\s+USUAL',
    ]
    
    for pattern in name_patterns:
        match = re.search(pattern, text_upper)
        if match:
            if len(match.groups()) == 1:
                extracted_data['nombre_completo'] = match.group(1).strip()
            else:
                # Si hay dos grupos, combinarlos
                name_part1 = match.group(1).strip()
                name_part2 = match.group(2).strip() if len(match.groups()) > 1 else ""
                extracted_data['nombre_completo'] = f"{name_part1} {name_part2}".strip()
            
            logger.info(f"👤 Nombre completo: {extracted_data['nombre_completo']}")
            break
    
    # 3. FECHAS (formato panameño con meses en español)
    date_patterns = [
        r'EXPEDIDA:\s*(\d{1,2}-[A-Z]{3}-\d{4})',  # 16-NOV-2017
        r'EXPIRA:\s*(\d{1,2}-[A-Z]{3}-\d{4})',   # 16-NOV-2027
        r'FECHA DE NACIMIENTO:\s*(\d{1,2}-[A-Z]{3}-\d{4})',  # 27-ABR-1964
    ]
    
    for pattern in date_patterns:
        matches = re.findall(pattern, text_upper)
        if matches:
            if 'EXPEDIDA' in pattern:
                extracted_data['fecha_emision'] = convert_spanish_date(matches[0])
            elif 'EXPIRA' in pattern:
                extracted_data['fecha_expiracion'] = convert_spanish_date(matches[0])
            elif 'NACIMIENTO' in pattern:
                extracted_data['fecha_nacimiento'] = convert_spanish_date(matches[0])
    
    # 4. GÉNERO
    gender_match = re.search(r'SEXO:\s*([MF])', text_upper)
    if gender_match:
        extracted_data['genero'] = gender_match.group(1)
    
    # 5. LUGAR DE NACIMIENTO
    birth_place_match = re.search(r'LUGAR DE NACIMIENTO:\s*([A-Z,\s]+)', text_upper)
    if birth_place_match:
        extracted_data['lugar_nacimiento'] = birth_place_match.group(1).strip()
    
    return extracted_data

def convert_spanish_date(date_str):
    """Convierte fechas en español a formato ISO"""
    if not date_str:
        return None
    
    # Mapeo de meses en español/inglés a números
    month_map = {
        'ENE': '01', 'FEB': '02', 'MAR': '03', 'ABR': '04', 'MAY': '05', 'JUN': '06',
        'JUL': '07', 'AGO': '08', 'SEP': '09', 'OCT': '10', 'NOV': '11', 'DIC': '12',
        'JAN': '01', 'APR': '04', 'AUG': '08', 'DEC': '12'
    }
    
    try:
        # Limpiar y dividir la fecha
        clean_date = re.sub(r'[^\w\s-]', '', date_str.strip())
        parts = re.split(r'[-\s]+', clean_date)
        
        if len(parts) == 3:
            day = parts[0].zfill(2)
            month_text = parts[1].upper()
            year = parts[2]
            
            if month_text in month_map:
                month = month_map[month_text]
                return f"{year}-{month}-{day}"
    
    except Exception as e:
        logger.warning(f"Error convirtiendo fecha '{date_str}': {str(e)}")
    
    return None

def clean_and_validate_data(extracted_data):
    """Limpia y valida los datos extraídos"""
    
    # Limpiar nombre completo
    if extracted_data.get('nombre_completo'):
        name = extracted_data['nombre_completo']
        # Eliminar texto no relevante
        name = re.sub(r'(SPECIMEN|MUESTRA|NOMBRE USUAL)', '', name)
        name = re.sub(r'\s+', ' ', name).strip()
        extracted_data['nombre_completo'] = name
    
    # Validar y limpiar número de identificación
    if extracted_data.get('numero_identificacion'):
        num_id = extracted_data['numero_identificacion']
        num_id = re.sub(r'[^\w-]', '', num_id)  # Conservar solo letras, números y guiones
        extracted_data['numero_identificacion'] = num_id
    
    return extracted_data

def validate_id_document_improved(extracted_data):
    """Validación mejorada con criterios más específicos"""
    validation = {
        'is_valid': True,
        'confidence': 0.8,  # Empezar con confianza alta
        'errors': [],
        'warnings': []
    }
    
    # ==================== VALIDACIONES CRÍTICAS ====================
    
    # 1. Tipo de documento
    if not extracted_data.get('tipo_identificacion') or extracted_data['tipo_identificacion'] == 'desconocido':
        validation['errors'].append("Tipo de documento no identificado")
        validation['is_valid'] = False
        validation['confidence'] -= 0.4
    
    # 2. Número de identificación
    if not extracted_data.get('numero_identificacion'):
        validation['errors'].append("Número de identificación no encontrado")
        validation['is_valid'] = False
        validation['confidence'] -= 0.3
    elif extracted_data['numero_identificacion'].startswith('AUTO-'):
        validation['errors'].append("Número de identificación generado automáticamente")
        validation['is_valid'] = False
        validation['confidence'] -= 0.5
    
    # 3. Nombre completo
    if not extracted_data.get('nombre_completo'):
        validation['errors'].append("Nombre completo no encontrado")
        validation['is_valid'] = False
        validation['confidence'] -= 0.2
    elif extracted_data['nombre_completo'] == 'Titular no identificado':
        validation['errors'].append("Nombre genérico asignado")
        validation['is_valid'] = False
        validation['confidence'] -= 0.3
    
    # ==================== VALIDACIONES ESPECÍFICAS POR TIPO ====================
    
    doc_type = extracted_data.get('tipo_identificacion')
    
    if doc_type == 'pasaporte':
        # Validar formato número pasaporte
        num_id = extracted_data.get('numero_identificacion', '')
        if not re.match(r'^[A-Z]{2}\d{7}$', num_id):
            validation['warnings'].append("Formato de número de pasaporte inusual")
            validation['confidence'] -= 0.1
        
        # Los pasaportes deben tener autoridad de emisión
        if not extracted_data.get('autoridad_emision'):
            validation['warnings'].append("Autoridad de emisión no detectada")
            validation['confidence'] -= 0.05
        
        # Validar país de emisión
        if not extracted_data.get('pais_emision') or extracted_data['pais_emision'] == 'Desconocido':
            validation['warnings'].append("País de emisión no identificado")
            validation['confidence'] -= 0.1
    
    elif doc_type == 'cedula_panama':
        # Validar formato cédula panameña
        num_id = extracted_data.get('numero_identificacion', '')
        if not re.match(r'^\d{1,2}-\d{3,4}-\d{1,4}$', num_id):
            validation['warnings'].append("Formato de cédula panameña incorrecto")
            validation['confidence'] -= 0.2
        
        # País debe ser Panamá
        if extracted_data.get('pais_emision') != 'Panamá':
            validation['warnings'].append("País de emisión incorrecto para cédula panameña")
            validation['confidence'] -= 0.1
    
    elif doc_type == 'dni':
        # Validar formato DNI español
        num_id = extracted_data.get

def lambda_handler(event, context):
    """
    Función principal OPTIMIZADA para procesar documentos de identidad
    """
    start_time = time.time()
    logger.info("="*80)
    logger.info("🚀 INICIANDO PROCESAMIENTO DE DOCUMENTO DE IDENTIDAD")
    logger.info("="*80)
    logger.info("Evento recibido: " + json.dumps(event))
    
    response = {
        'procesados': 0,
        'errores': 0,
        'requieren_revision': 0,
        'detalles': []
    }

    for record in event['Records']:
        documento_detalle = {
            'documento_id': None,
            'estado': 'sin_procesar',
            'tiempo': 0,
            'tipo_detectado': None,
            'datos_extraidos': False
        }
        
        record_start = time.time()
        registro_id = None
        
        try:
            # ==================== PARSEAR MENSAJE ====================
            
            message_body = json.loads(record['body'])
            document_id = message_body['document_id']
            documento_detalle['documento_id'] = document_id
            
            logger.info(f"📄 Procesando documento: {document_id}")
            
            # Iniciar registro de procesamiento
            registro_id = log_document_processing_start(
                document_id, 
                'procesamiento_identidad_optimizado',
                datos_entrada=message_body
            )
            
            # ==================== OBTENER DATOS DE LA BD ====================
            
            logger.info(f"📥 Recuperando datos extraídos de la base de datos...")
            document_data_result = get_extracted_data_from_db(document_id)
            
            if not document_data_result:
                raise Exception(f"No se pudieron recuperar datos del documento {document_id}")
            
            extracted_text = document_data_result['extracted_data'].get('texto_completo')
            if not extracted_text:
                raise Exception(f"No hay texto extraído disponible para documento {document_id}")
            
            logger.info(f"📖 Texto recuperado: {len(extracted_text)} caracteres")
            
            # ==================== EXTRACCIÓN MEJORADA ====================
            
            logger.info(f"🔍 Iniciando extracción de datos de identificación...")
            
            entidades = document_data_result['extracted_data'].get('entidades')
            metadatos = document_data_result['extracted_data'].get('metadatos_extraccion')
            
            # Registrar sub-proceso de extracción
            sub_registro_id = log_document_processing_start(
                document_id, 
                'extraccion_datos_identidad',
                datos_entrada={"texto_longitud": len(extracted_text)},
                analisis_id=registro_id
            )
            
            # USAR LA FUNCIÓN MEJORADA
            id_data = extract_id_document_data(extracted_text, entidades, metadatos)
            
            tipo_detectado = id_data.get('tipo_identificacion', 'desconocido')
            documento_detalle['tipo_detectado'] = tipo_detectado
            
            # Finalizar registro de extracción
            log_document_processing_end(
                sub_registro_id, 
                estado='completado',
                datos_procesados={
                    "tipo_detectado": tipo_detectado,
                    "numero_extraido": bool(id_data.get('numero_identificacion')),
                    "nombre_extraido": bool(id_data.get('nombre_completo')),
                    "campos_totales": len([k for k, v in id_data.items() if v is not None])
                }
            )
            
            # ==================== VALIDACIÓN MEJORADA ====================
            
            logger.info(f"✅ Validando datos extraídos...")
            validation = validate_id_document_improved(id_data)
            confidence = validation['confidence']
            
            # Evaluar si requiere revisión manual
            requires_review = evaluate_confidence(
                confidence,
                document_type=tipo_detectado,
                validation_results=validation
            )
            
            if requires_review:
                documento_detalle['estado'] = 'requiere_revision'
                response['requieren_revision'] += 1
                logger.warning(f"⚠️ Documento {document_id} requiere revisión manual")
                
                # Marcar para revisión manual
                try:
                    mark_for_manual_review(
                        document_id=document_id,
                        analysis_id=registro_id,
                        confidence=confidence,
                        document_type=tipo_detectado,
                        validation_info=validation,
                        extracted_data=id_data
                    )
                except Exception as review_error:
                    logger.error(f"Error al marcar para revisión: {str(review_error)}")
            
            # ==================== GUARDAR EN BASE DE DATOS ====================
            
            # Solo intentar guardar si tenemos datos mínimos válidos
            should_save = (
                id_data.get('numero_identificacion') and 
                not id_data['numero_identificacion'].startswith('AUTO-') and
                id_data.get('nombre_completo') and 
                id_data['nombre_completo'] != 'Titular no identificado'
            )
            
            if should_save:
                logger.info(f"💾 Guardando datos extraídos en base de datos...")
                
                db_registro_id = log_document_processing_start(
                    document_id, 
                    'guardar_datos_identidad',
                    datos_entrada={
                        "tipo_documento": tipo_detectado,
                        "confidence": confidence,
                        "valid": validation['is_valid']
                    },
                    analisis_id=registro_id
                )
                
                # USAR LA FUNCIÓN MEJORADA DE REGISTRO
                success = register_document_identification_improved(document_id, id_data)
                
                if success:
                    logger.info(f"✅ Datos guardados exitosamente")
                    documento_detalle['datos_extraidos'] = True
                    
                    log_document_processing_end(db_registro_id, estado='completado')
                    
                    # Mostrar cambios si los hay
                    log_identification_changes(document_id)
                else:
                    logger.error(f"❌ Error al guardar datos")
                    documento_detalle['error_guardado'] = "Falló el guardado en BD"
                    
                    log_document_processing_end(
                        db_registro_id, 
                        estado='error',
                        mensaje_error="Error al guardar en base de datos"
                    )
            else:
                logger.warning(f"⚠️ Datos insuficientes para guardar, marcando para revisión manual")
                documento_detalle['estado'] = 'datos_insuficientes'
                response['requieren_revision'] += 1
                
                # Actualizar estado a revisión manual
                update_document_processing_status(
                    document_id, 
                    'requiere_revision_manual',
                    f"Datos extraídos insuficientes. Número: {id_data.get('numero_identificacion')}, Nombre: {id_data.get('nombre_completo')}"
                )
            
            # ==================== ACTUALIZAR DOCUMENTO PRINCIPAL ====================
            
            update_id = log_document_processing_start(
                document_id, 
                'actualizar_documento_principal',
                datos_entrada={"confidence": confidence, "is_valid": validation['is_valid']},
                analisis_id=registro_id
            )
            
            try:
                update_document_extraction_data_with_type_preservation(
                    document_id,
                    json.dumps(id_data, ensure_ascii=False),
                    confidence,
                    validation['is_valid']
                )
                
                log_document_processing_end(update_id, estado='completado')
                logger.info(f"📄 Documento principal actualizado")
                
            except Exception as update_error:
                log_document_processing_end(
                    update_id, 
                    estado='error',
                    mensaje_error=str(update_error)
                )
                logger.error(f"Error al actualizar documento principal: {str(update_error)}")
            
            # ==================== ACTUALIZAR ESTADO FINAL ====================
            
            if documento_detalle['estado'] == 'sin_procesar':
                documento_detalle['estado'] = 'procesado'
                response['procesados'] += 1
            
            # Determinar estado final
            if requires_review or not should_save:
                status = 'requiere_revision_manual'
                message = "Documento procesado - Requiere revisión manual"
            elif validation['is_valid']:
                status = 'completado'
                message = "Documento de identidad procesado correctamente"
            else:
                status = 'completado_con_advertencias'
                message = "Documento procesado con advertencias"
            
            # Obtener tipo de documento para la actualización de estado
            tipo_doc_map = {
                'dni': 'DNI',
                'cedula_panama': 'Cédula',
                'cedula': 'Cédula',
                'pasaporte': 'Pasaporte'
            }
            tipo_normalizado = tipo_doc_map.get(tipo_detectado, 'Documento de Identidad')
            
            final_details = {
                'validación': validation,
                'tipo_detectado': tipo_detectado,
                'campos_extraídos': [k for k, v in id_data.items() if v is not None],
                'requires_review': requires_review,
                'datos_guardados': should_save
            }
            
            update_document_processing_status(
                document_id, 
                status, 
                json.dumps(final_details, ensure_ascii=False),
                tipo_documento=tipo_normalizado
            )
            
            documento_detalle['confianza'] = confidence
            documento_detalle['estado_final'] = status
            
            # Finalizar registro principal exitosamente
            log_document_processing_end(
                registro_id, 
                estado='completado',
                confianza=confidence,
                datos_salida=final_details,
                mensaje_error=None if validation['is_valid'] else "Procesado con advertencias"
            )
            
            logger.info(f"✅ Documento {document_id} procesado completamente")
            logger.info(f"   📋 Tipo: {tipo_detectado}")
            logger.info(f"   📊 Confianza: {confidence:.2f}")
            logger.info(f"   📝 Estado: {status}")
            
        except Exception as e:
            error_msg = str(e)
            logger.error(f"❌ Error procesando documento {document_id if 'document_id' in locals() else 'DESCONOCIDO'}: {error_msg}")
            logger.error(traceback.format_exc())
            
            documento_detalle['estado'] = 'error'
            documento_detalle['error'] = error_msg
            response['errores'] += 1
            
            # Actualizar estado de error
            if 'document_id' in locals():
                try:
                    update_document_processing_status(
                        document_id, 
                        'error',
                        f"Error en procesamiento de identidad: {error_msg}"
                    )
                except:
                    pass
            
            # Finalizar registro con error
            if registro_id:
                log_document_processing_end(
                    registro_id, 
                    estado='error',
                    mensaje_error=error_msg
                )
                
        finally:
            # Calcular tiempo de procesamiento
            tiempo_procesamiento = time.time() - record_start
            documento_detalle['tiempo'] = tiempo_procesamiento
            response['detalles'].append(documento_detalle)

    # ==================== ASIGNAR CARPETA (SOLO SI HAY ÉXITOS) ====================
    
    if response['procesados'] > 0:
        try:
            # Obtener último documento procesado exitosamente
            last_success_doc = None
            for detalle in response['detalles']:
                if detalle['estado'] in ['procesado', 'requiere_revision']:
                    last_success_doc = detalle['documento_id']
                    break
            
            if last_success_doc:
                cliente_id = get_client_id_by_document(last_success_doc)
                if cliente_id:
                    logger.info(f"👤 Asignando carpeta para cliente {cliente_id}")
                    assign_folder_and_link(cliente_id, last_success_doc)
                else:
                    logger.warning(f"⚠️ No se encontró cliente para documento {last_success_doc}")
        except Exception as assign_error:
            logger.error(f"❌ Error al asignar carpeta: {str(assign_error)}")
    
    # ==================== RESUMEN FINAL ====================
    
    total_time = time.time() - start_time
    response['tiempo_total'] = total_time
    response['total_registros'] = len(event['Records'])
    
    logger.info("="*80)
    logger.info("📊 RESUMEN DEL PROCESAMIENTO")
    logger.info("="*80)
    logger.info(f"✅ Documentos procesados exitosamente: {response['procesados']}")
    logger.info(f"⚠️ Documentos que requieren revisión: {response['requieren_revision']}")
    logger.info(f"❌ Documentos con errores: {response['errores']}")
    logger.info(f"⏱️ Tiempo total: {total_time:.2f} segundos")
    
    if response['procesados'] > 0 or response['requieren_revision'] > 0:
        logger.info("🎉 Procesamiento completado con resultados")
    else:
        logger.warning("⚠️ Procesamiento completado SIN documentos exitosos")
    
    return {
        'statusCode': 200,
        'body': json.dumps(response, ensure_ascii=False)
    }