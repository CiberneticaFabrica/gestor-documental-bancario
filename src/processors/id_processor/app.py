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
    get_client_id_by_document,
    generate_uuid
)

from common.flow_utilis import crear_instancia_flujo_documento
 
# Configurar el logger
logger = logging.getLogger()
logger.setLevel(os.environ.get('LOG_LEVEL', 'INFO'))

# Patrones regex para extraer información de documentos de identidad
DNI_PATTERN = r'(?i)(?:DNI|Documento Nacional de Identidad)[^\d]*(\d{8}[A-Z]?)'
PASSPORT_PATTERN = r'[A-Z]{2}\d{7}'
NAME_PATTERN = r'(?i)(?:Nombre|Name)[^\w]*([\w\s]+)'
SURNAME_PATTERN = r'(?i)(?:Apellidos|Surname)[^\w]*([\w\s]+)'
DOB_PATTERN = r'(?i)(?:Fecha de nacimiento|Date of birth)[^\d]*(\d{1,2}[/-]\d{1,2}[/-]\d{2,4})'
EXPIRY_PATTERN = r'(?i)(?:Fecha de caducidad|Date of expiry)[^\d]*(\d{1,2}[/-]\d{1,2}[/-]\d{2,4})'
NATIONALITY_PATTERN = r'(?i)(?:Nacionalidad|Nationality)[^\w]*([\w\s]+)'
PANAMA_ID_PATTERN = r'\b(\d{1,2}-\d{3,4}-\d{1,4})\b'
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
  # ✅ USAR EXTRACTOR ROBUSTO PARA NOMBRES
    if not extracted_data.get('nombre_completo'):
        name_result = extract_name_universal(text, 'cedula_panama')
        if name_result:
            extracted_data['nombre_completo'] = name_result['nombre_completo']
            extracted_data['nombre'] = name_result.get('nombre')
            extracted_data['apellidos'] = name_result.get('apellidos')
            logger.info(f"✅ Nombre extraído con patrón robusto: {name_result['pattern_used']}")
    
    # Si no se extrajo con el robusto, intentar patrones básicos como fallback
    if not extracted_data.get('nombre_completo'):
        basic_patterns = [
            r'TRIBUNAL ELECTORAL\s+([A-Z\s]+)\s+NOMBRE USUAL',
            r'ELECTORAL\s+([A-Z\s]+)\s+NOMBRE',
            r'P\s+A\s+([A-Z\s]+)\s+N\s+A\s+([A-Z\s]+)\s+M',
        ]
        
        for pattern in basic_patterns:
            match = re.search(pattern, text_upper)
            if match:
                if len(match.groups()) == 1:
                    extracted_data['nombre_completo'] = match.group(1).strip()
                else:
                    name_part1 = match.group(1).strip()
                    name_part2 = match.group(2).strip() if len(match.groups()) > 1 else ""
                    extracted_data['nombre_completo'] = f"{name_part1} {name_part2}".strip()
                
                logger.info(f"👤 Nombre completo (fallback): {extracted_data['nombre_completo']}")
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
                extracted_data['fecha_emision'] = convert_spanish_date_improved(matches[0])
            elif 'EXPIRA' in pattern:
                extracted_data['fecha_expiracion'] = convert_spanish_date_improved(matches[0])
            elif 'NACIMIENTO' in pattern:
                extracted_data['fecha_nacimiento'] = convert_spanish_date_improved(matches[0])
    
    # 4. GÉNERO
    gender_match = re.search(r'SEXO:\s*([MF])', text_upper)
    if gender_match:
        extracted_data['genero'] = gender_match.group(1)
    
    # 5. LUGAR DE NACIMIENTO
    birth_place_match = re.search(r'LUGAR DE NACIMIENTO:\s*([A-Z,\s]+)', text_upper)
    if birth_place_match:
        extracted_data['lugar_nacimiento'] = birth_place_match.group(1).strip()
    
    return extracted_data

#nuevas validaciones
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
    # ✅ AGREGAR ESTA VALIDACIÓN NUEVA:
    elif len(extracted_data['nombre_completo']) < 6:
        validation['warnings'].append("Nombre muy corto, posible extracción incompleta")
        validation['confidence'] -= 0.1
    elif not re.search(r'[A-Za-z]', extracted_data['nombre_completo']):
        validation['warnings'].append("Nombre sin letras válidas")
        validation['confidence'] -= 0.2
    
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
        num_id = extracted_data.get('numero_identificacion', '')
        if not re.match(r'^\d{8}[A-Z]$', num_id):
            validation['warnings'].append("Formato de DNI español incorrecto")
            validation['confidence'] -= 0.2
    
    # ==================== VALIDACIONES DE FECHAS ====================
    
    fecha_emision = extracted_data.get('fecha_emision')
    fecha_expiracion = extracted_data.get('fecha_expiracion')
    
    # Validar formato de fechas
    date_pattern = r'^\d{4}-\d{2}-\d{2}$'
    
    if fecha_emision and not re.match(date_pattern, str(fecha_emision)):
        validation['errors'].append("Formato de fecha de emisión inválido")
        validation['confidence'] -= 0.1
    
    if fecha_expiracion and not re.match(date_pattern, str(fecha_expiracion)):
        validation['errors'].append("Formato de fecha de expiración inválido")
        validation['confidence'] -= 0.1
    
    # Validar lógica de fechas
    if fecha_emision and fecha_expiracion:
        try:
            from datetime import datetime
            emision_dt = datetime.strptime(str(fecha_emision), '%Y-%m-%d')
            expiracion_dt = datetime.strptime(str(fecha_expiracion), '%Y-%m-%d')
            
            if emision_dt >= expiracion_dt:
                validation['errors'].append("Fecha de emisión posterior a fecha de expiración")
                validation['confidence'] -= 0.2
            
            # Verificar si el documento ha expirado
            now = datetime.now()
            if expiracion_dt < now:
                validation['warnings'].append("El documento ha expirado")
                validation['confidence'] -= 0.05
                
        except ValueError:
            validation['errors'].append("Fechas con formato incorrecto")
            validation['confidence'] -= 0.1
    
    # ==================== AJUSTE FINAL DE CONFIANZA ====================
    
    # Asegurar que la confianza esté entre 0 y 1
    validation['confidence'] = max(0.0, min(1.0, validation['confidence']))
    
    # Si hay errores críticos, marcar como inválido
    if len(validation['errors']) > 0:
        validation['is_valid'] = False
    
    logger.info(f"📊 Validación completada - Confianza: {validation['confidence']:.2f}")
    if validation['errors']:
        logger.error(f"❌ Errores encontrados: {'; '.join(validation['errors'])}")
    if validation['warnings']:
        logger.warning(f"⚠️ Advertencias: {'; '.join(validation['warnings'])}")
    
    return validation

def format_date_panama_improved(date_str):
    """
    Convierte fechas en formato panameño (DD-mes-YYYY) a formato ISO (YYYY-MM-DD)
    VERSIÓN MEJORADA que maneja fechas incompletas
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
        
        day = parts[0].strip()
        month_text = parts[1].strip().lower()
        year = parts[2].strip()
        
        # ✅ CORRECCIÓN: Validar y corregir día inválido
        try:
            day_int = int(day)
            if day_int == 0 or day_int > 31:
                logger.warning(f"Día inválido {day_int}, usando día 01")
                day = "01"
            else:
                day = day.zfill(2)
        except ValueError:
            logger.warning(f"Día no numérico '{day}', usando día 01")
            day = "01"
        
        # Convertir mes a número
        if month_text in month_map:
            month = month_map[month_text]
        else:
            # Si no se encuentra el mes exacto, intentar con las primeras tres letras
            month_abbr = month_text[:3]
            if month_abbr in month_map:
                month = month_map[month_abbr]
            else:
                logger.warning(f"Mes no reconocido: {month_text}, usando enero")
                month = "01"
        
        # ✅ CORRECCIÓN: Validar año
        try:
            year_int = int(year)
            if year_int < 1900 or year_int > 2100:
                logger.warning(f"Año inválido {year_int}")
                return None
            year = str(year_int)
        except ValueError:
            logger.warning(f"Año no numérico '{year}'")
            return None
        
        # ✅ VALIDACIÓN FINAL: Verificar que la fecha sea válida
        from datetime import datetime
        try:
            # Intentar crear la fecha para validarla
            test_date = datetime.strptime(f"{year}-{month}-{day}", '%Y-%m-%d')
            formatted_date = f"{year}-{month}-{day}"
            logger.info(f"✅ Fecha convertida: '{date_str}' → '{formatted_date}'")
            return formatted_date
        except ValueError as ve:
            logger.warning(f"Fecha resultante inválida: {year}-{month}-{day}, error: {ve}")
            # Como último recurso, usar el primer día del mes
            try:
                test_date = datetime.strptime(f"{year}-{month}-01", '%Y-%m-%d')
                formatted_date = f"{year}-{month}-01"
                logger.warning(f"Usando primer día del mes: '{formatted_date}'")
                return formatted_date
            except ValueError:
                logger.error(f"No se pudo crear fecha válida para: {date_str}")
                return None
        
    except Exception as e:
        logger.warning(f"Error al procesar fecha '{date_str}': {str(e)}")
        return None

def normalize_date_improved(date_str, default_day="01"):
    """
    Normaliza fechas de diferentes formatos a ISO YYYY-MM-DD
    VERSIÓN MEJORADA que maneja fechas incompletas o malformadas
    """
    if not date_str:
        return None
    
    # Limpiar la cadena
    clean_date = str(date_str).strip()
    
    # Si ya está en formato ISO, validarla
    if re.match(r'^\d{4}-\d{2}-\d{2}$', clean_date):
        # Verificar que no tenga día/mes 00
        year, month, day = clean_date.split('-')
        
        if month == "00":
            month = "01"
            logger.warning(f"Mes 00 corregido a 01 en fecha: {clean_date}")
        
        if day == "00":
            day = default_day
            logger.warning(f"Día 00 corregido a {default_day} en fecha: {clean_date}")
        
        corrected_date = f"{year}-{month}-{day}"
        
        # Validar que la fecha sea real
        try:
            from datetime import datetime
            datetime.strptime(corrected_date, '%Y-%m-%d')
            return corrected_date
        except ValueError:
            logger.warning(f"Fecha inválida después de corrección: {corrected_date}")
            return None
    
    # Si tiene formato panameño, usar la función específica
    if re.search(r'\d{1,2}[-\s][a-zA-Zéúíóá]+[-\s]\d{4}', clean_date):
        return format_date_panama_improved(clean_date)
    
    # Otros formatos internacionales
    formats_to_try = [
        '%d/%m/%Y',
        '%m/%d/%Y', 
        '%d.%m.%Y',
        '%Y/%m/%d',
        '%d %m %Y',
        '%Y-%m-%d'
    ]
    
    for fmt in formats_to_try:
        try:
            from datetime import datetime
            date_obj = datetime.strptime(clean_date, fmt)
            return date_obj.strftime('%Y-%m-%d')
        except ValueError:
            continue
    
    logger.warning(f"No se pudo normalizar la fecha: {date_str}")
    return None

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
    if not extracted_data.get('nombre_completo'):
        name_result = extract_name_universal(text, 'pasaporte')
        if name_result:
            extracted_data['nombre_completo'] = name_result['nombre_completo']
            extracted_data['nombre'] = name_result.get('nombre')
            extracted_data['apellidos'] = name_result.get('apellidos')
            logger.info(f"✅ Nombre de pasaporte extraído: {name_result['nombre_completo']}")
            logger.info(f"   Patrón usado: {name_result['pattern_used']}")
    
    # Si no se extrajo con el robusto, intentar patrones básicos como fallback
    if not extracted_data.get('nombre_completo'):
        basic_name_patterns = [
            r'APELLIDOS/SURNAME\s+([A-Z\s]+)\s+NOMBRES\s*/\s*GIVEN\s+NAMES\s+([A-Z\s]+?)(?:\s+SPECIMEN|\s+\d|\s+[A-Z]{3}|$)',
            r'SURNAME\s+([A-Z\s]+)\s+GIVEN\s+NAMES\s+([A-Z\s]+?)(?:\s+SPECIMEN|\s+\d|\s+[A-Z]{3}|$)',
        ]
        
        for pattern in basic_name_patterns:
            match = re.search(pattern, text_upper)
            if match and len(match.groups()) >= 2:
                apellidos = match.group(1).strip()
                nombres = match.group(2).strip()
                
                extracted_data['apellidos'] = apellidos
                extracted_data['nombre'] = nombres
                extracted_data['nombre_completo'] = f"{nombres} {apellidos}"
                logger.info(f"👤 Nombre completo (fallback): {extracted_data['nombre_completo']}")
                break
    
    # 3. PAÍS DE EMISIÓN
    country_patterns = [
        r'REPUBLICA DE PANAMA|REPUBLIC OF PANAMA',
        r'ESPAÑA|SPAIN',
        r'COLOMBIA',
        r'ESTADOS UNIDOS|UNITED STATES',
        r'MEXICO|MÉXICO',
    ]
    
    for pattern in country_patterns:
        if re.search(pattern, text_upper):
            if 'PANAMA' in pattern:
                extracted_data['pais_emision'] = 'Panamá'
            elif 'ESPAÑA' in pattern or 'SPAIN' in pattern:
                extracted_data['pais_emision'] = 'España'
            elif 'COLOMBIA' in pattern:
                extracted_data['pais_emision'] = 'Colombia'
            elif 'ESTADOS UNIDOS' in pattern or 'UNITED STATES' in pattern:
                extracted_data['pais_emision'] = 'Estados Unidos'
            elif 'MEXICO' in pattern or 'MÉXICO' in pattern:
                extracted_data['pais_emision'] = 'México'
            logger.info(f"🌍 País de emisión: {extracted_data['pais_emision']}")
            break
    
    # 4. FECHAS (emisión y expiración) - MEJORADO
    date_patterns = [
        r'DATE OF ISSUE\s+(\d{1,2}\s+[A-Z]{3}\s+\d{4})',  # 02 ENE 2014
        r'FECHA DE EXPEDICION\s+(\d{1,2}\s+[A-Z]{3}\s+\d{4})',
        r'DATE OF EXPIRY\s+(\d{1,2}\s+[A-Z]{3}\s+\d{4})',  # 02 ENE 2019
        r'FECHA DE VENCIMIENTO\s+(\d{1,2}\s+[A-Z]{3}\s+\d{4})',
        # Patrones adicionales para diferentes formatos
        r'ISSUED:\s*(\d{1,2}[-\s][A-Z]{3}[-\s]\d{4})',
        r'EXPIRES:\s*(\d{1,2}[-\s][A-Z]{3}[-\s]\d{4})',
    ]
    
    for pattern in date_patterns:
        matches = re.findall(pattern, text_upper)
        if matches:
            if 'ISSUE' in pattern or 'EXPEDICION' in pattern or 'ISSUED' in pattern:
                # Usar función mejorada
                extracted_data['fecha_emision'] = normalize_date_improved(matches[0])
                logger.info(f"📅 Fecha emisión: {extracted_data['fecha_emision']}")
            elif 'EXPIRY' in pattern or 'VENCIMIENTO' in pattern or 'EXPIRES' in pattern:
                extracted_data['fecha_expiracion'] = normalize_date_improved(matches[0])
                logger.info(f"📅 Fecha expiración: {extracted_data['fecha_expiracion']}")
    
    # 5. GÉNERO
    gender_patterns = [
        r'SEXO/SEX\s+([MF])',
        r'SEX\s+([MF])',
        r'SEXO\s+([MF])',
        r'GENDER\s+([MF])',
        r'([MF])\s+(?:MALE|FEMALE|MASCULINO|FEMENINO)',
    ]
    
    for pattern in gender_patterns:
        match = re.search(pattern, text_upper)
        if match:
            extracted_data['genero'] = match.group(1)
            logger.info(f"👥 Género: {extracted_data['genero']}")
            break
    
    # 6. FECHA DE NACIMIENTO
    birth_date_patterns = [
        r'DATE OF BIRTH\s+(\d{1,2}\s+[A-Z]{3}\s+\d{4})',  # 21 MAR 1991
        r'FECHA DE NACIMIENTO\s+(\d{1,2}\s+[A-Z]{3}\s+\d{4})',
        r'BORN:\s*(\d{1,2}[-\s][A-Z]{3}[-\s]\d{4})',
        r'DOB\s+(\d{1,2}\s+[A-Z]{3}\s+\d{4})',
        # Patrón general de fecha (usado con cuidado)
        r'(?:NACIMIENTO|BIRTH|BORN).*?(\d{1,2}\s+[A-Z]{3}\s+\d{4})',
    ]
    
    for pattern in birth_date_patterns:
        match = re.search(pattern, text_upper)
        if match:
            extracted_data['fecha_nacimiento'] = normalize_date_improved(match.group(1))
            logger.info(f"🎂 Fecha nacimiento: {extracted_data['fecha_nacimiento']}")
            break
    
    # 7. AUTORIDAD DE EMISIÓN
    authority_patterns = [
        r'AUTORIDAD[:\s]+([A-Z\s/]+?)(?:\s+AUTHORITY|\s*$)',
        r'AUTHORITY[:\s]+([A-Z\s/]+?)(?:\s+AUTORIDAD|\s*$)',
        r'ISSUED BY[:\s]+([A-Z\s/]+?)(?:\s+EMITIDO|\s*$)',
        r'EMITIDO POR[:\s]+([A-Z\s/]+?)(?:\s+ISSUED|\s*$)',
    ]
    
    # Verificar patrones específicos primero
    if 'PASAPORTES/PANAMA' in text_upper or 'PASSPORTS/PANAMA' in text_upper:
        extracted_data['autoridad_emision'] = 'Pasaportes/Panamá'
        logger.info(f"🏛️ Autoridad: {extracted_data['autoridad_emision']}")
    elif 'MINISTERIO DEL INTERIOR' in text_upper:
        extracted_data['autoridad_emision'] = 'Ministerio del Interior'
        logger.info(f"🏛️ Autoridad: {extracted_data['autoridad_emision']}")
    else:
        # Buscar con patrones generales
        for pattern in authority_patterns:
            match = re.search(pattern, text_upper)
            if match:
                authority = match.group(1).strip()
                # Limpiar texto innecesario
                authority = re.sub(r'(SPECIMEN|MUESTRA)', '', authority).strip()
                if len(authority) > 5:  # Solo si tiene contenido significativo
                    extracted_data['autoridad_emision'] = authority
                    logger.info(f"🏛️ Autoridad: {extracted_data['autoridad_emision']}")
                    break
    
    # 8. NACIONALIDAD
    nationality_patterns = [
        r'NACIONALIDAD[:\s/]+([A-Z]+)',
        r'NATIONALITY[:\s/]+([A-Z]+)',
        r'NATIONAL[:\s]+([A-Z]+)',
        r'CITIZEN[:\s]+([A-Z]+)',
    ]
    
    for pattern in nationality_patterns:
        match = re.search(pattern, text_upper)
        if match:
            nationality = match.group(1).strip()
            # Convertir códigos comunes a nombres completos
            nationality_map = {
                'PANAMENA': 'Panameña',
                'PANAMANIAN': 'Panameña',
                'ESPANOLA': 'Española',
                'SPANISH': 'Española',
                'COLOMBIANA': 'Colombiana',
                'COLOMBIAN': 'Colombiana',
                'MEXICANA': 'Mexicana',
                'MEXICAN': 'Mexicana',
                'ESTADOUNIDENSE': 'Estadounidense',
                'AMERICAN': 'Estadounidense',
                'USA': 'Estadounidense',
                'PAN': 'Panameña',
                'ESP': 'Española',
                'COL': 'Colombiana',
                'MEX': 'Mexicana',
            }
            
            extracted_data['nacionalidad'] = nationality_map.get(nationality, nationality)
            logger.info(f"🏳️ Nacionalidad: {extracted_data['nacionalidad']}")
            break
    
    # 9. LUGAR DE NACIMIENTO
    birth_place_patterns = [
        r'(?:PLACE OF BIRTH|LUGAR DE NACIMIENTO)[:\s]+([A-ZÁÉÍÓÚÑ\s,]+?)(?:\s+LIEU|\s+DATE|\s*$)',
        r'(?:LIEU DE NAISSANCE)[:\s]+([A-ZÁÉÍÓÚÑ\s,]+?)(?:\s+PLACE|\s+DATE|\s*$)',
        r'BORN IN[:\s]+([A-ZÁÉÍÓÚÑ\s,]+?)(?:\s+DATE|\s*$)',
        r'NACIDO EN[:\s]+([A-ZÁÉÍÓÚÑ\s,]+?)(?:\s+FECHA|\s*$)',
    ]
    
    for pattern in birth_place_patterns:
        match = re.search(pattern, text_upper)
        if match:
            place = match.group(1).strip()
            # Limpiar texto innecesario
            place = re.sub(r'(SPECIMEN|MUESTRA)', '', place).strip()
            if len(place) > 3:  # Solo si tiene contenido significativo
                extracted_data['lugar_nacimiento'] = place
                logger.info(f"🏠 Lugar de nacimiento: {extracted_data['lugar_nacimiento']}")
                break
    
    # 10. VALIDACIONES ADICIONALES PARA PASAPORTES
    
    # Si se detectó un pasaporte pero no se encontró país de emisión, intentar inferirlo
    if not extracted_data.get('pais_emision') and extracted_data.get('numero_identificacion'):
        numero = extracted_data['numero_identificacion']
        # Inferir país por prefijo del número
        if numero.startswith('PA'):
            extracted_data['pais_emision'] = 'Panamá'
            logger.info(f"🌍 País inferido por prefijo: Panamá")
        elif numero.startswith('ES'):
            extracted_data['pais_emision'] = 'España'
            logger.info(f"🌍 País inferido por prefijo: España")
    
    # Si no se encontró autoridad pero sí país, asignar autoridad típica
    if not extracted_data.get('autoridad_emision') and extracted_data.get('pais_emision'):
        pais = extracted_data['pais_emision']
        if pais == 'Panamá':
            extracted_data['autoridad_emision'] = 'Pasaportes/Panamá'
        elif pais == 'España':
            extracted_data['autoridad_emision'] = 'Policía Nacional'
        elif pais == 'Colombia':
            extracted_data['autoridad_emision'] = 'Cancillería'
        
        if extracted_data.get('autoridad_emision'):
            logger.info(f"🏛️ Autoridad inferida: {extracted_data['autoridad_emision']}")
    
    return extracted_data

def extract_id_document_data_improved_core(text, entidades=None, metadatos=None):
    """
    Función principal de extracción de datos de documentos de identidad
    VERSIÓN MEJORADA con mejores patrones y lógica de detección
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
        extracted_data = extract_dni_spain_data_improved(text, text_upper, extracted_data)
    else:
        extracted_data = extract_generic_id_data_improved(text, text_upper, extracted_data)
    
    # ==================== FALLBACK UNIVERSAL PARA NOMBRES ====================
    
    # Si NINGÚN método anterior extrajo nombre, intentar con el extractor universal
    if not extracted_data.get('nombre_completo'):
        logger.warning(f"⚠️ Nombre no extraído con métodos específicos, intentando extractor universal...")
        
        # Intentar con tipo detectado
        name_result = extract_name_universal(text, extracted_data.get('tipo_identificacion'))
        
        if not name_result:
            # Intentar sin tipo específico (todos los patrones)
            name_result = extract_name_universal(text, None)
        
        if name_result:
            extracted_data['nombre_completo'] = name_result['nombre_completo']
            extracted_data['nombre'] = name_result.get('nombre')
            extracted_data['apellidos'] = name_result.get('apellidos')
            logger.info(f"🔧 Nombre rescatado con extractor universal: {name_result['nombre_completo']}")
            logger.info(f"   Patrón usado: {name_result['pattern_used']}")
            logger.info(f"   Confianza: {name_result['confidence']}")
        else:
            logger.error(f"❌ NO se pudo extraer nombre con ningún método")
    # ==================== VALIDACIÓN Y LIMPIEZA FINAL ====================
    
    # Limpiar y validar datos extraídos
    extracted_data = clean_and_validate_data_improved(extracted_data)
    
    return extracted_data

def extract_dni_spain_data_improved(text, text_upper, extracted_data):
    """Extracción mejorada para DNI español"""
    
    extracted_data['pais_emision'] = 'España'
    
    # 1. NÚMERO DE DNI
    dni_patterns = [
        r'(?i)DNI[:\s]*(\d{8}[A-Z])',
        r'(?i)DOCUMENTO[:\s]+(\d{8}[A-Z])',
        r'\b(\d{8}[A-Z])\b',
        r'(?i)NACIONAL DE IDENTIDAD[:\s]*(\d{8}[A-Z])',
    ]
    
    for pattern in dni_patterns:
        match = re.search(pattern, text)
        if match:
            extracted_data['numero_identificacion'] = match.group(1)
            logger.info(f"📝 Número de DNI: {extracted_data['numero_identificacion']}")
            break
    
    # 2. VALIDAR Y LIMPIAR NÚMERO DE IDENTIFICACIÓN
    if extracted_data.get('numero_identificacion'):
        num_id = extracted_data['numero_identificacion']
        # Conservar solo letras, números y guiones
        num_id = re.sub(r'[^\w-]', '', num_id)
        # Remover espacios extras
        num_id = re.sub(r'\s+', '', num_id)
        
        # Validar que tiene contenido significativo
        if len(num_id) >= 5:
            extracted_data['numero_identificacion'] = num_id
        else:
            logger.warning(f"Número de identificación muy corto: '{num_id}'")
            extracted_data['numero_identificacion'] = None
    
    # 3. LIMPIAR CAMPOS DE TEXTO LIBRE
    text_fields = ['lugar_nacimiento', 'autoridad_emision', 'nacionalidad']
    for field in text_fields:
        if extracted_data.get(field):
            value = extracted_data[field]
            # Limpiar texto innecesario
            value = re.sub(r'(SPECIMEN|MUESTRA)', '', value, flags=re.IGNORECASE)
            value = re.sub(r'\s+', ' ', value).strip()
            
            # Solo mantener si tiene contenido significativo
            if len(value) >= 3:
                extracted_data[field] = value
            else:
                extracted_data[field] = None
    
    # 4. VALIDAR GÉNERO
    if extracted_data.get('genero'):
        genero = extracted_data['genero'].upper()
        if genero in ['M', 'F', 'MALE', 'FEMALE', 'MASCULINO', 'FEMENINO']:
            # Normalizar a M/F
            if genero in ['MALE', 'MASCULINO']:
                extracted_data['genero'] = 'M'
            elif genero in ['FEMALE', 'FEMENINO']:
                extracted_data['genero'] = 'F'
            else:
                extracted_data['genero'] = genero[0]  # Tomar primera letra
        else:
            logger.warning(f"Género no reconocido: {genero}")
            extracted_data['genero'] = None
    
    # 5. VALIDAR PAÍS DE EMISIÓN
    if extracted_data.get('pais_emision'):
        pais = extracted_data['pais_emision']
        # Mapeo de países comunes
        pais_map = {
            'PANAMA': 'Panamá',
            'PANAMÁ': 'Panamá',
            'SPAIN': 'España',
            'ESPAÑA': 'España',
            'COLOMBIA': 'Colombia',
            'MEXICO': 'México',
            'MÉXICO': 'México',
            'UNITED STATES': 'Estados Unidos',
            'USA': 'Estados Unidos',
        }
        
        pais_upper = pais.upper()
        if pais_upper in pais_map:
            extracted_data['pais_emision'] = pais_map[pais_upper]
        # Si no está en el mapeo pero tiene contenido válido, mantenerlo
        elif len(pais) >= 3 and pais.replace(' ', '').isalpha():
            extracted_data['pais_emision'] = pais.title()
        else:
            extracted_data['pais_emision'] = None
    
    # 6. INFERIR DATOS FALTANTES BASADOS EN TIPO DE DOCUMENTO
    doc_type = extracted_data.get('tipo_identificacion')
    
    if doc_type == 'cedula_panama' and not extracted_data.get('pais_emision'):
        extracted_data['pais_emision'] = 'Panamá'
        logger.info("🌍 País inferido: Panamá (por tipo de documento)")
    
    elif doc_type == 'dni' and not extracted_data.get('pais_emision'):
        extracted_data['pais_emision'] = 'España'
        extracted_data['nacionalidad'] = 'Española'
        logger.info("🌍 País y nacionalidad inferidos: España/Española (por DNI)")
    
    # 7. VALIDAR COHERENCIA DE FECHAS
    fecha_emision = extracted_data.get('fecha_emision')
    fecha_expiracion = extracted_data.get('fecha_expiracion')
    fecha_nacimiento = extracted_data.get('fecha_nacimiento')
    
    if fecha_emision and fecha_expiracion:
        try:
            from datetime import datetime
            emision_dt = datetime.strptime(fecha_emision, '%Y-%m-%d')
            expiracion_dt = datetime.strptime(fecha_expiracion, '%Y-%m-%d')
            
            # La fecha de expiración debe ser posterior a la de emisión
            if emision_dt >= expiracion_dt:
                logger.warning(f"Fechas incoherentes: emisión {fecha_emision} >= expiración {fecha_expiracion}")
                # Mantener solo la fecha que parezca más confiable
                if abs((emision_dt - datetime.now()).days) > abs((expiracion_dt - datetime.now()).days):
                    extracted_data['fecha_emision'] = None
                else:
                    extracted_data['fecha_expiracion'] = None
        except ValueError:
            logger.warning("Error al validar coherencia de fechas")
    
    if fecha_nacimiento:
        try:
            from datetime import datetime
            nacimiento_dt = datetime.strptime(fecha_nacimiento, '%Y-%m-%d')
            now = datetime.now()
            
            # Validar que la fecha de nacimiento sea razonable
            age = (now - nacimiento_dt).days / 365.25
            if age < 0 or age > 120:
                logger.warning(f"Fecha de nacimiento inválida: {fecha_nacimiento} (edad: {age:.1f} años)")
                extracted_data['fecha_nacimiento'] = None
        except ValueError:
            logger.warning(f"Formato de fecha de nacimiento inválido: {fecha_nacimiento}")
            extracted_data['fecha_nacimiento'] = None
    
    # 8. ASEGURAR TIPO DE DOCUMENTO VÁLIDO
    if extracted_data.get('tipo_identificacion') == 'desconocido':
        # Intentar última inferencia basada en datos disponibles
        if extracted_data.get('numero_identificacion'):
            numero = extracted_data['numero_identificacion']
            if re.match(r'^[A-Z]{2}\d{7}', numero):
                extracted_data['tipo_identificacion'] = 'dni'
                logger.info(f"🪪 DNI detectado (score: 3)")
            elif re.match(r'^[A-Z]{2}\d{7}', numero):
                extracted_data['tipo_identificacion'] = 'pasaporte'
                logger.info(f"📔 PASAPORTE detectado (score: 3)")
    
    # 2. NOMBRE COMPLETO
    name_patterns = [
        r'(?i)NOMBRE[:\s]+([A-ZÁÉÍÓÚÑ\s]+?)(?:\s+APELLIDOS|\s+FECHA|\s*$)',
        r'(?i)APELLIDOS[:\s]+([A-ZÁÉÍÓÚÑ\s]+?)(?:\s+NOMBRE|\s+FECHA|\s*$)',
        # Patrón para nombre completo junto
        r'(?i)(?:NOMBRE COMPLETO|TITULAR)[:\s]+([A-ZÁÉÍÓÚÑ\s]+?)(?:\s+FECHA|\s+DNI|\s*$)',
    ]
    
    for pattern in name_patterns:
        match = re.search(pattern, text)
        if match:
            extracted_data['nombre_completo'] = match.group(1).strip()
            logger.info(f"👤 Nombre completo: {extracted_data['nombre_completo']}")
            break
    
    # 3. FECHAS
    date_patterns = [
        r'(?i)(?:FECHA DE EXPEDICIÓN|EXPEDIDO)[:\s]*(\d{1,2}[/-]\d{1,2}[/-]\d{2,4})',
        r'(?i)(?:FECHA DE CADUCIDAD|VÁLIDO HASTA)[:\s]*(\d{1,2}[/-]\d{1,2}[/-]\d{2,4})',
        r'(?i)(?:FECHA DE NACIMIENTO|NACIDO)[:\s]*(\d{1,2}[/-]\d{1,2}[/-]\d{2,4})',
    ]
    
    for pattern in date_patterns:
        matches = re.findall(pattern, text)
        if matches:
            if 'EXPEDICIÓN' in pattern or 'EXPEDIDO' in pattern:
                extracted_data['fecha_emision'] = normalize_date_improved(matches[0])
            elif 'CADUCIDAD' in pattern or 'VÁLIDO' in pattern:
                extracted_data['fecha_expiracion'] = normalize_date_improved(matches[0])
            elif 'NACIMIENTO' in pattern or 'NACIDO' in pattern:
                extracted_data['fecha_nacimiento'] = normalize_date_improved(matches[0])
    
    # 4. LUGAR DE NACIMIENTO
    birth_place_match = re.search(r'(?i)(?:LUGAR DE NACIMIENTO|NACIDO EN)[:\s]+([A-ZÁÉÍÓÚÑ\s,]+)', text)
    if birth_place_match:
        extracted_data['lugar_nacimiento'] = birth_place_match.group(1).strip()
    
    # 5. NACIONALIDAD (siempre española para DNI)
    extracted_data['nacionalidad'] = 'Española'
    
    return extracted_data

def extract_generic_id_data_improved(text, text_upper, extracted_data):
    """Extracción genérica mejorada para documentos no identificados específicamente"""
    
    # 1. PATRONES GENÉRICOS PARA NÚMEROS DE IDENTIFICACIÓN
    generic_id_patterns = [
        r'(?i)(?:ID|IDENTIFICATION)[:\s]*([A-Z0-9]{5,15})',
        r'(?i)(?:NÚMERO|NUMBER)[:\s]*([A-Z0-9]{5,15})',
        r'(?i)(?:DOC|DOCUMENTO)[:\s]*([A-Z0-9]{5,15})',
        r'(?i)(?:IDENTITY|IDENTIDAD)[:\s]*([A-Z0-9]{5,15})',
        # Patrones para formatos comunes
        r'\b([A-Z]{2,3}\d{6,8})\b',  # Formato pasaporte genérico
        r'\b(\d{7,10})\b',  # Número genérico largo
    ]
    
    for pattern in generic_id_patterns:
        match = re.search(pattern, text)
        if match:
            potential_id = match.group(1)
            # Validar que no sea una fecha u otro dato
            if not re.match(r'^\d{1,2}[/-]\d{1,2}[/-]\d{2,4}$', potential_id):
                extracted_data['numero_identificacion'] = potential_id
                logger.info(f"📝 Número genérico encontrado: {potential_id}")
                break
    
    # 2. PATRONES GENÉRICOS PARA NOMBRES
    generic_name_patterns = [
        r'(?i)(?:NOMBRE|NAME)[:\s]+([A-ZÁÉÍÓÚÑ\s]+?)(?:\s+(?:APELLIDO|SURNAME)|\s+\d|\s*$)',
        r'(?i)(?:TITULAR|HOLDER)[:\s]+([A-ZÁÉÍÓÚÑ\s]+?)(?:\s+\d|\s*$)',
        r'(?i)(?:FULL NAME|NOMBRE COMPLETO)[:\s]+([A-ZÁÉÍÓÚÑ\s]+?)(?:\s+\d|\s*$)',
    ]
    
    for pattern in generic_name_patterns:
        match = re.search(pattern, text)
        if match:
            extracted_data['nombre_completo'] = match.group(1).strip()
            logger.info(f"👤 Nombre genérico encontrado: {extracted_data['nombre_completo']}")
            break
    
    # 3. PATRONES GENÉRICOS PARA FECHAS
    generic_date_patterns = [
        r'(?i)(?:ISSUED|EMITIDO|EXPEDIDO)[:\s]*(\d{1,2}[/-]\d{1,2}[/-]\d{2,4})',
        r'(?i)(?:EXPIRES|EXPIRA|VENCE)[:\s]*(\d{1,2}[/-]\d{1,2}[/-]\d{2,4})',
        r'(?i)(?:VALID UNTIL|VÁLIDO HASTA)[:\s]*(\d{1,2}[/-]\d{1,2}[/-]\d{2,4})',
    ]
    
    for pattern in generic_date_patterns:
        matches = re.findall(pattern, text)
        if matches:
            if 'ISSUED' in pattern or 'EMITIDO' in pattern or 'EXPEDIDO' in pattern:
                extracted_data['fecha_emision'] = normalize_date_improved(matches[0])
            elif any(word in pattern for word in ['EXPIRES', 'EXPIRA', 'VENCE', 'VALID']):
                extracted_data['fecha_expiracion'] = normalize_date_improved(matches[0])
    
    # 4. INTENTAR DETECTAR EL TIPO DE DOCUMENTO POR CONTEXTO
    if extracted_data.get('numero_identificacion'):
        numero = extracted_data['numero_identificacion']
        
        # Si tiene formato de pasaporte
        if re.match(r'^[A-Z]{2}\d{7}$', numero):
            extracted_data['tipo_identificacion'] = 'pasaporte'
            logger.info(f"📔 Tipo inferido por formato de número: pasaporte")
        
        # Si tiene formato de cédula panameña
        elif re.match(r'^\d{1,2}-\d{3,4}-\d{1,4}$', numero):
            extracted_data['tipo_identificacion'] = 'cedula_panama'
            extracted_data['pais_emision'] = 'Panamá'
            logger.info(f"🆔 Tipo inferido por formato de número: cédula panameña")
        
        # Si tiene formato de DNI español
        elif re.match(r'^\d{8}[A-Z]$', numero):
            extracted_data['tipo_identificacion'] = 'dni'
            extracted_data['pais_emision'] = 'España'
            extracted_data['nacionalidad'] = 'Española'
            logger.info(f"🪪 Tipo inferido por formato de número: DNI español")
    
    return extracted_data

def clean_and_validate_data_improved(extracted_data):
    """Limpia y valida los datos extraídos - VERSIÓN MEJORADA"""
    
    # 1. LIMPIAR NOMBRE COMPLETO
    if extracted_data.get('nombre_completo'):
        name = extracted_data['nombre_completo']
        # Eliminar texto no relevante
        name = re.sub(r'(SPECIMEN|MUESTRA|NOMBRE USUAL)', '', name, flags=re.IGNORECASE)
        name = re.sub(r'\s+', ' ', name).strip()
        # Remover caracteres especiales innecesarios
        name = re.sub(r'[^\w\s\-\']', '', name)
        
        # Validar que tiene contenido significativo
        if len(name) >= 3 and not name.isdigit():
            extracted_data['nombre_completo'] = name
        else:
            logger.warning(f"Nombre limpiado resulta inválido: '{name}'")
            extracted_data['nombre_completo'] = None
    
    # 2. LIMPIAR NÚMERO DE IDENTIFICACIÓN
    if extracted_data.get('numero_identificacion'):
        num_id = extracted_data['numero_identificacion']
        # Conservar solo letras, números y guiones
        num_id = re.sub(r'[^\w-]', '', num_id)
        # Remover espacios extras
        num_id = re.sub(r'\s+', '', num_id)
        
        # Validar que tiene contenido significativo
        if len(num_id) >= 5:
            extracted_data['numero_identificacion'] = num_id
        else:
            logger.warning(f"Número de identificación muy corto: '{num_id}'")
            extracted_data['numero_identificacion'] = None
    
    # 3. LIMPIAR CAMPOS DE TEXTO LIBRE
    text_fields = ['lugar_nacimiento', 'autoridad_emision', 'nacionalidad']
    for field in text_fields:
        if extracted_data.get(field):
            value = extracted_data[field]
            # Limpiar texto innecesario
            value = re.sub(r'(SPECIMEN|MUESTRA)', '', value, flags=re.IGNORECASE)
            value = re.sub(r'\s+', ' ', value).strip()
            
            # Solo mantener si tiene contenido significativo
            if len(value) >= 3:
                extracted_data[field] = value
            else:
                extracted_data[field] = None
    
    # 4. VALIDAR GÉNERO
    if extracted_data.get('genero'):
        genero = extracted_data['genero'].upper()
        if genero in ['M', 'F', 'MALE', 'FEMALE', 'MASCULINO', 'FEMENINO']:
            # Normalizar a M/F
            if genero in ['MALE', 'MASCULINO']:
                extracted_data['genero'] = 'M'
            elif genero in ['FEMALE', 'FEMENINO']:
                extracted_data['genero'] = 'F'
            else:
                extracted_data['genero'] = genero[0]  # Tomar primera letra
        else:
            logger.warning(f"Género no reconocido: {genero}")
            extracted_data['genero'] = None
    
    return extracted_data

def convert_spanish_date_improved(date_str):
    """Convierte fechas en español a formato ISO - VERSIÓN MEJORADA"""
    if not date_str:
        return None
    
    # Mapeo de meses en español/inglés a números
    month_map = {
        'ENE': '01', 'FEB': '02', 'MAR': '03', 'ABR': '04', 'MAY': '05', 'JUN': '06',
        'JUL': '07', 'AGO': '08', 'SEP': '09', 'OCT': '10', 'NOV': '11', 'DIC': '12',
        'JAN': '01', 'APR': '04', 'AUG': '08', 'DEC': '12',
        'ENERO': '01', 'FEBRERO': '02', 'MARZO': '03', 'ABRIL': '04', 'MAYO': '05', 'JUNIO': '06',
        'JULIO': '07', 'AGOSTO': '08', 'SEPTIEMBRE': '09', 'OCTUBRE': '10', 'NOVIEMBRE': '11', 'DICIEMBRE': '12'
    }
    
    try:
        # Limpiar y dividir la fecha
        clean_date = re.sub(r'[^\w\s-]', '', date_str.strip())
        parts = re.split(r'[-\s]+', clean_date)
        
        if len(parts) == 3:
            day = parts[0].strip()
            month_text = parts[1].upper().strip()
            year = parts[2].strip()
            
            # ✅ VALIDAR DÍA
            try:
                day_int = int(day)
                if day_int == 0 or day_int > 31:
                    logger.warning(f"Día inválido {day_int}, usando 01")
                    day = "01"
                else:
                    day = str(day_int).zfill(2)
            except ValueError:
                day = "01"
            
            # ✅ VALIDAR MES
            if month_text in month_map:
                month = month_map[month_text]
            else:
                # Buscar coincidencia parcial
                for month_name, month_num in month_map.items():
                    if month_text in month_name or month_name in month_text:
                        month = month_num
                        break
                else:
                    logger.warning(f"Mes no reconocido: {month_text}, usando 01")
                    month = "01"
            
            # ✅ VALIDAR AÑO
            try:
                year_int = int(year)
                if year_int < 1900 or year_int > 2100:
                    logger.warning(f"Año inválido: {year_int}")
                    return None
                year = str(year_int)
            except ValueError:
                logger.warning(f"Año inválido: {year}")
                return None
            
            # ✅ VALIDAR FECHA FINAL
            from datetime import datetime
            try:
                test_date = datetime.strptime(f"{year}-{month}-{day}", '%Y-%m-%d')
                final_date = f"{year}-{month}-{day}"
                logger.info(f"✅ Fecha convertida: '{date_str}' → '{final_date}'")
                return final_date
            except ValueError:
                # Usar primer día del mes como fallback
                try:
                    test_date = datetime.strptime(f"{year}-{month}-01", '%Y-%m-%d')
                    final_date = f"{year}-{month}-01"
                    logger.warning(f"Fecha corregida a: {final_date}")
                    return final_date
                except ValueError:
                    logger.error(f"No se pudo crear fecha válida para: {date_str}")
                    return None
    
    except Exception as e:
        logger.warning(f"Error convirtiendo fecha '{date_str}': {str(e)}")
    
    return None

def extract_name_universal(text, document_type=None):
    """
    Función UNIVERSAL para extraer nombres de documentos de identidad.
    Maneja TODOS los formatos posibles de documentos panameños, españoles y pasaportes.
    
    Args:
        text (str): Texto extraído del documento
        document_type (str): Tipo de documento ('cedula_panama', 'dni', 'pasaporte', etc.)
    
    Returns:
        dict: {
            'nombre_completo': str,
            'nombre': str,
            'apellidos': str,
            'confidence': float,
            'pattern_used': str
        }
    """
    
    if not text:
        return None
    
    # Limpiar y normalizar texto
    clean_text = normalize_text_for_extraction(text)
    
    # Intentar extracción específica por tipo de documento primero
    if document_type:
        result = extract_name_by_document_type(clean_text, document_type)
        if result:
            return result
    
    # Si no se especifica tipo o falla, intentar todos los patrones
    return extract_name_with_all_patterns(clean_text)

def normalize_text_for_extraction(text):
    """Normaliza el texto para mejor extracción"""
    # Convertir a mayúsculas
    text = text.upper()
    
    # Normalizar espacios múltiples
    text = re.sub(r'\s+', ' ', text)
    
    # Limpiar caracteres problemáticos pero mantener estructura
    text = re.sub(r'[^\w\s\-/]', ' ', text)
    
    return text.strip()

def extract_name_by_document_type(text, document_type):
    """Extrae nombres usando patrones específicos por tipo de documento"""
    
    if document_type in ['cedula_panama', 'cedula']:
        return extract_name_cedula_panama(text)
    elif document_type == 'pasaporte':
        return extract_name_pasaporte(text)
    elif document_type == 'dni':
        return extract_name_dni_spain(text)
    
    return None

def extract_name_cedula_panama(text):
    """
    Extracción ROBUSTA para cédulas panameñas.
    Maneja TODOS los formatos observados en los ejemplos.
    """
    
    # PATRONES ORDENADOS POR PRIORIDAD Y ESPECIFICIDAD
    patterns = [
        # Patrón 1: Formato estándar con TRIBUNAL ELECTORAL
        {
            'pattern': r'TRIBUNAL\s+ELECTORAL\s+([A-Z][A-Z\s]+?)\s+(?:NOMBRE\s+USUAL|FECHA\s+DE\s+NACIMIENTO)',
            'name': 'tribunal_electoral_standard',
            'confidence': 0.95
        },
        
        # Patrón 2: ELECTORAL seguido de nombre
        {
            'pattern': r'ELECTORAL\s+([A-Z][A-Z\s]+?)\s+(?:NOMBRE\s+USUAL|FECHA)',
            'name': 'electoral_simple',
            'confidence': 0.90
        },
        
        # Patrón 3: Formato con DOCUMENTO DE IDENTIDAD + número
        {
            'pattern': r'DOCUMENTO\s+DE\s+IDENTIDAD\s+\d+\s+([A-Z][A-Z\s]+?)\s+NOMBRE\s+USUAL',
            'name': 'documento_identidad_numbered',
            'confidence': 0.92
        },
        
        # Patrón 4: Formato P A ... N A ... M (visto en ejemplos)
        {
            'pattern': r'P\s+A\s+([A-Z][A-Z\s]+?)\s+N\s+A\s+([A-Z][A-Z\s]+?)\s+M',
            'name': 'format_p_a_n_a_m',
            'confidence': 0.85,
            'special_handler': 'handle_p_a_n_a_format'
        },
        
        # Patrón 5: Nombre antes de NOMBRE USUAL (más flexible)
        {
            'pattern': r'([A-Z]{2,}(?:\s+[A-Z]{2,}){1,4})\s+NOMBRE\s+USUAL',
            'name': 'before_nombre_usual',
            'confidence': 0.80
        },
        
        # Patrón 6: Entre PANAMA y NOMBRE USUAL
        {
            'pattern': r'PANAMA\s+([A-Z][A-Z\s]+?)\s+NOMBRE\s+USUAL',
            'name': 'panama_to_nombre',
            'confidence': 0.85
        },
        
        # Patrón 7: Nombre seguido de cédula (formato alternativo)
        {
            'pattern': r'([A-Z]{2,}(?:\s+[A-Z]{2,}){1,4})\s+\d{1,2}-\d{3,4}-\d{1,4}',
            'name': 'name_before_cedula',
            'confidence': 0.75
        },
        
        # Patrón 8: Captura entre palabras clave comunes
        {
            'pattern': r'(?:REPUBLICA\s+DE\s+PANAMA|TRIBUNAL|ELECTORAL)\s+.*?([A-Z]{2,}(?:\s+[A-Z]{2,}){1,4})\s+(?:NOMBRE|FECHA|LUGAR)',
            'name': 'between_keywords',
            'confidence': 0.70
        }
    ]
    
    for pattern_info in patterns:
        try:
            matches = re.finditer(pattern_info['pattern'], text)
            
            for match in matches:
                if pattern_info.get('special_handler'):
                    # Manejar patrones especiales
                    if pattern_info['special_handler'] == 'handle_p_a_n_a_format':
                        result = handle_p_a_n_a_format(match)
                    else:
                        continue
                else:
                    # Manejo estándar
                    extracted_name = match.group(1).strip()
                    result = validate_and_clean_name(extracted_name)
                
                if result:
                    result['pattern_used'] = pattern_info['name']
                    result['confidence'] = pattern_info['confidence']
                    logger.info(f"✅ Nombre extraído con patrón '{pattern_info['name']}': {result['nombre_completo']}")
                    return result
                    
        except Exception as e:
            logger.warning(f"Error en patrón {pattern_info['name']}: {str(e)}")
            continue
    
    logger.warning("❌ No se pudo extraer nombre con ningún patrón de cédula panameña")
    return None

def extract_name_pasaporte(text):
    """Extracción ROBUSTA para pasaportes"""
    
    patterns = [
        # Patrón 1: APELLIDOS/SURNAME ... NOMBRES/GIVEN NAMES
        {
            'pattern': r'(?:APELLIDOS|SURNAME)\s*[/]*\s*([A-Z\s]+?)\s+(?:NOMBRES|GIVEN\s+NAMES)\s*[/]*\s*([A-Z\s]+?)(?:\s+(?:SPECIMEN|FECHA|DATE|\d))',
            'name': 'apellidos_nombres_format',
            'confidence': 0.95,
            'type': 'apellidos_nombres'
        },
        
        # Patrón 2: SURNAME ... GIVEN NAMES (formato internacional)
        {
            'pattern': r'SURNAME\s+([A-Z\s]+?)\s+GIVEN\s+NAMES\s+([A-Z\s]+?)(?:\s+(?:SPECIMEN|DATE|\d))',
            'name': 'surname_given_names',
            'confidence': 0.90,
            'type': 'apellidos_nombres'
        },
        
        # Patrón 3: Línea MRZ (Machine Readable Zone)
        {
            'pattern': r'P<[A-Z]{3}([A-Z]+)<([A-Z<]+?)<<',
            'name': 'mrz_format',
            'confidence': 0.85,
            'type': 'mrz',
            'special_handler': 'handle_mrz_format'
        },
        
        # Patrón 4: Nombre en línea específica de pasaporte
        {
            'pattern': r'(?:PASSPORT|PASAPORTE)\s+(?:NO|N[Oº])\s*[A-Z0-9]+\s*([A-Z\s]+?)(?:\s+(?:NATIONALITY|FECHA))',
            'name': 'after_passport_number',
            'confidence': 0.75
        }
    ]
    
    for pattern_info in patterns:
        try:
            match = re.search(pattern_info['pattern'], text)
            if match:
                if pattern_info.get('special_handler') == 'handle_mrz_format':
                    result = handle_mrz_format(match)
                elif pattern_info.get('type') == 'apellidos_nombres':
                    # Formato apellidos/nombres
                    apellidos = clean_name_component(match.group(1))
                    nombres = clean_name_component(match.group(2))
                    
                    if apellidos and nombres:
                        result = {
                            'nombre_completo': f"{nombres} {apellidos}",
                            'nombre': nombres,
                            'apellidos': apellidos,
                            'confidence': pattern_info['confidence'],
                            'pattern_used': pattern_info['name']
                        }
                        logger.info(f"✅ Nombre de pasaporte extraído: {result['nombre_completo']}")
                        return result
                else:
                    # Formato simple
                    extracted_name = match.group(1).strip()
                    result = validate_and_clean_name(extracted_name)
                    if result:
                        result['pattern_used'] = pattern_info['name']
                        result['confidence'] = pattern_info['confidence']
                        return result
                        
        except Exception as e:
            logger.warning(f"Error en patrón de pasaporte {pattern_info['name']}: {str(e)}")
            continue
    
    return None

def extract_name_dni_spain(text):
    """Extracción para DNI español"""
    
    patterns = [
        {
            'pattern': r'(?:NOMBRE|NAME)\s*[:\s]+([A-ZÁÉÍÓÚÑ\s]+?)(?:\s+(?:APELLIDOS|SURNAME|FECHA|DNI|\d))',
            'name': 'nombre_field',
            'confidence': 0.90
        },
        {
            'pattern': r'(?:APELLIDOS|SURNAME)\s*[:\s]+([A-ZÁÉÍÓÚÑ\s]+?)(?:\s+(?:NOMBRE|NAME|FECHA|\d))',
            'name': 'apellidos_field',
            'confidence': 0.90
        },
        {
            'pattern': r'(?:TITULAR|HOLDER)\s*[:\s]+([A-ZÁÉÍÓÚÑ\s]+?)(?:\s+(?:FECHA|DNI|\d))',
            'name': 'titular_field',
            'confidence': 0.85
        }
    ]
    
    for pattern_info in patterns:
        try:
            match = re.search(pattern_info['pattern'], text)
            if match:
                extracted_name = match.group(1).strip()
                result = validate_and_clean_name(extracted_name)
                if result:
                    result['pattern_used'] = pattern_info['name']
                    result['confidence'] = pattern_info['confidence']
                    return result
                    
        except Exception as e:
            logger.warning(f"Error en patrón DNI {pattern_info['name']}: {str(e)}")
            continue
    
    return None

def extract_name_with_all_patterns(text):
    """Intenta todos los patrones disponibles como último recurso"""
    
    # Intentar patrones de cada tipo de documento
    for doc_type in ['cedula_panama', 'pasaporte', 'dni']:
        result = extract_name_by_document_type(text, doc_type)
        if result:
            result['pattern_used'] = f"fallback_{doc_type}"
            result['confidence'] = max(0.5, result.get('confidence', 0.5) - 0.1)  # Reducir confianza
            return result
    
    return None

def handle_p_a_n_a_format(match):
    """Maneja el formato especial P A ... N A ... M visto en cédulas"""
    try:
        # En este formato, los grupos suelen ser apellidos y nombres
        apellidos = clean_name_component(match.group(1))
        nombres = clean_name_component(match.group(2)) if len(match.groups()) > 1 else None
        
        if apellidos:
            if nombres:
                return {
                    'nombre_completo': f"{nombres} {apellidos}",
                    'nombre': nombres,
                    'apellidos': apellidos
                }
            else:
                return validate_and_clean_name(apellidos)
    except:
        pass
    return None

def handle_mrz_format(match):
    """Maneja formato MRZ de pasaportes"""
    try:
        # En MRZ, < separa apellidos de nombres
        apellidos_raw = match.group(1)
        nombres_raw = match.group(2)
        
        # Limpiar < y convertir a espacios
        apellidos = apellidos_raw.replace('<', ' ').strip()
        nombres = nombres_raw.replace('<', ' ').strip()
        
        if apellidos and nombres:
            return {
                'nombre_completo': f"{nombres} {apellidos}",
                'nombre': nombres,
                'apellidos': apellidos
            }
    except:
        pass
    return None

def validate_and_clean_name(name_text):
    """
    Valida y limpia un nombre extraído.
    Retorna dict con nombre procesado o None si no es válido.
    """
    if not name_text or len(name_text.strip()) < 3:
        return None
    
    # Limpiar el nombre
    cleaned = clean_name_component(name_text)
    
    if not cleaned:
        return None
    
    # Validar que parece un nombre real
    if not is_valid_name(cleaned):
        return None
    
    # Intentar separar nombres y apellidos si es posible
    parts = cleaned.split()
    
    if len(parts) >= 4:
        # Asumir que los primeros 2 son nombres y el resto apellidos
        nombres = ' '.join(parts[:2])
        apellidos = ' '.join(parts[2:])
    elif len(parts) == 3:
        # Asumir que el primero es nombre y el resto apellidos
        nombres = parts[0]
        apellidos = ' '.join(parts[1:])
    elif len(parts) == 2:
        # Asumir primer nombre y apellido
        nombres = parts[0]
        apellidos = parts[1]
    else:
        # Solo un componente
        nombres = cleaned
        apellidos = None
    
    return {
        'nombre_completo': cleaned,
        'nombre': nombres,
        'apellidos': apellidos
    }

def clean_name_component(text):
    """Limpia un componente de nombre individual"""
    if not text:
        return None
    
    # Convertir a title case y limpiar
    text = text.strip().title()
    
    # Remover palabras no válidas
    invalid_words = [
        'Specimen', 'Muestra', 'Documento', 'Identidad', 'Tribunal', 
        'Electoral', 'Republica', 'Panama', 'Passport', 'Pasaporte',
        'Nombre', 'Usual', 'Fecha', 'Nacimiento', 'Apellidos', 'Surname',
        'Given', 'Names', 'Authority', 'Autoridad'
    ]
    
    words = text.split()
    clean_words = [w for w in words if w not in invalid_words and len(w) > 1]
    
    result = ' '.join(clean_words).strip()
    return result if len(result) > 2 else None

def is_valid_name(name):
    """Valida que un texto parece ser un nombre real"""
    if not name or len(name) < 3:
        return False
    
    # Debe contener solo letras, espacios y algunos caracteres especiales
    if not re.match(r'^[A-ZÁÉÍÓÚÑa-záéíóúñ\s\-\'\.]+$', name):
        return False
    
    # Debe tener al menos una letra
    if not re.search(r'[A-Za-z]', name):
        return False
    
    # No debe ser todo mayúsculas de menos de 3 caracteres
    words = name.split()
    if len(words) < 1:
        return False
    
    # Cada palabra debe tener al menos 2 caracteres
    if any(len(word) < 2 for word in words):
        return False
    
    # No debe contener números
    if re.search(r'\d', name):
        return False
    
    return True

# Función de conveniencia para integración con código existente
def extract_name_from_document(text, document_type=None):
    """
    Función de conveniencia que mantiene compatibilidad con código existente.
    Retorna solo el nombre completo o None.
    """
    result = extract_name_universal(text, document_type)
    return result['nombre_completo'] if result else None

def validar_fecha(fecha: str) -> bool:
    formatos = ["%d-%b-%Y", "%d-%B-%Y", "%d-%m-%Y", "%Y-%m-%d"]
    for fmt in formatos:
        try:
            datetime.strptime(fecha.strip(), fmt)
            return True
        except:
            continue
    return False

def elegir_nombre(nombre_q, apellido_q, nombre_extraido):
    if nombre_q and apellido_q and apellido_q.lower() not in nombre_q.lower():
        combinado = f"{nombre_q} {apellido_q}".strip()
    else:
        combinado = nombre_q or ""

    if nombre_extraido and len(nombre_extraido.split()) > len(combinado.split()):
        logger.info(f"Nombre más completo encontrado en texto: {nombre_extraido}")
        return nombre_extraido.strip()
    
    return combinado or nombre_extraido

def elegir_valor(valor_q, valor_texto, campo, validacion=None):
    if validacion:
        if validacion(valor_q):
            return valor_q
        elif validacion(valor_texto):
            logger.info(f"{campo} del texto validado mejor que query.")
            return valor_texto
    else:
        if valor_q:
            return valor_q
        elif valor_texto:
            logger.info(f"{campo} tomado del texto porque no se encontró en query.")
            return valor_texto
    return None

def reconciliar_datos_identidad(query_answers, texto_extraido_dict):
    resultado = {}

    resultado["nombre_completo"] = elegir_nombre(
        query_answers.get("nombre_completo", {}).get("answer"),
        query_answers.get("apellido_completo", {}).get("answer"),
        texto_extraido_dict.get("nombre_completo")
    )

    resultado["sexo"] = elegir_valor(
        query_answers.get("sexo", {}).get("answer"),
        texto_extraido_dict.get("sexo"),
        "sexo",
        lambda x: x not in [None, "NC", "N/A", ""]
    )

    resultado["fecha_nacimiento"] = elegir_valor(
        query_answers.get("fecha_nacimiento", {}).get("answer"),
        texto_extraido_dict.get("fecha_nacimiento"),
        "fecha_nacimiento",
        validar_fecha
    )

    resultado["fecha_expedicion"] = elegir_valor(
        query_answers.get("fecha_expedicion", {}).get("answer"),
        texto_extraido_dict.get("fecha_emision"),  # puede venir como emision
        "fecha_expedicion",
        validar_fecha
    )

    resultado["lugar_nacimiento"] = elegir_valor(
        query_answers.get("lugar_nacimiento", {}).get("answer"),
        texto_extraido_dict.get("lugar_nacimiento"),
        "lugar_nacimiento"
    )

    return resultado

def reconcile_identity_data(extracted_data, metadatos_extraccion):
    """
    Reconcilia datos extraídos del texto con query_answers de metadatos.
    Prioriza los datos más completos y precisos.
    
    Args:
        extracted_data (dict): Datos extraídos directamente del texto OCR
        metadatos_extraccion (dict): Metadatos que incluyen query_answers
    
    Returns:
        dict: Datos reconciliados con los mejores valores disponibles
    """
    
    # Obtener query_answers de metadatos
    query_answers = {}
    if isinstance(metadatos_extraccion, dict):
        query_answers = metadatos_extraccion.get('query_answers', {})
    elif isinstance(metadatos_extraccion, str):
        try:
            metadatos_dict = json.loads(metadatos_extraccion)
            query_answers = metadatos_dict.get('query_answers', {})
        except json.JSONDecodeError:
            logger.warning("No se pudo decodificar metadatos_extraccion")
            query_answers = {}
    
    if not query_answers:
        logger.info("No hay query_answers disponibles, usando solo datos extraídos del texto")
        return extracted_data
    
    logger.info("🔄 Iniciando reconciliación de datos...")
    
    # Crear copia de datos extraídos para modificar
    reconciled_data = extracted_data.copy()
    
    # ==================== RECONCILIACIÓN DE NOMBRES ====================
    
    nombre_completo_final = reconcile_full_name(
        extracted_data.get('nombre_completo'),
        query_answers.get('nombre_completo', {}).get('answer'),
        query_answers.get('apellido_completo', {}).get('answer'),
        query_answers.get('nombre_completo', {}).get('confidence', 0),
        query_answers.get('apellido_completo', {}).get('confidence', 0)
    )
    
    if nombre_completo_final:
        reconciled_data['nombre_completo'] = nombre_completo_final
        
        # Intentar separar nombres y apellidos del nombre completo final
        name_parts = split_full_name(nombre_completo_final)
        if name_parts:
            reconciled_data['nombre'] = name_parts.get('nombres')
            reconciled_data['apellidos'] = name_parts.get('apellidos')
    
    # ==================== RECONCILIACIÓN DE OTROS CAMPOS ====================
    
    # Sexo/Género
    reconciled_data['genero'] = reconcile_simple_field(
        extracted_data.get('genero'),
        query_answers.get('sexo', {}).get('answer'),
        'género',
        validator=lambda x: x in ['M', 'F', 'NC'] if x else False
    )
    
    # Fecha de nacimiento
    reconciled_data['fecha_nacimiento'] = reconcile_date_field(
        extracted_data.get('fecha_nacimiento'),
        query_answers.get('fecha_nacimiento', {}).get('answer'),
        'fecha_nacimiento'
    )
    
    # Fecha de expedición/emisión
    fecha_expedicion_query = query_answers.get('fecha_expedicion', {}).get('answer')
    reconciled_data['fecha_emision'] = reconcile_date_field(
        extracted_data.get('fecha_emision'),
        fecha_expedicion_query,
        'fecha_expedicion'
    )
    
    # Lugar de nacimiento
    reconciled_data['lugar_nacimiento'] = reconcile_simple_field(
        extracted_data.get('lugar_nacimiento'),
        query_answers.get('lugar_nacimiento', {}).get('answer'),
        'lugar_nacimiento',
        validator=lambda x: len(x) > 2 if x else False
    )
    
    # ==================== VALIDACIÓN FINAL ====================
    
    # Asegurar coherencia de número de identificación
    if not reconciled_data.get('numero_identificacion'):
        # Intentar extraer de entidades detectadas
        numero_inferido = extract_id_from_text_or_entities(extracted_data.get('texto_completo', ''))
        if numero_inferido:
            reconciled_data['numero_identificacion'] = numero_inferido
            logger.info(f"📝 Número de identificación inferido: {numero_inferido}")
    
    # Log de cambios realizados
    log_reconciliation_changes(extracted_data, reconciled_data)
    
    return reconciled_data


def reconcile_full_name(texto_name, query_nombre, query_apellido, conf_nombre=0, conf_apellido=0):
    """
    Reconcilia el nombre completo priorizando la información más completa.
    """
    
    # Limpiar valores
    texto_name = clean_name_value(texto_name)
    query_nombre = clean_name_value(query_nombre)
    query_apellido = clean_name_value(query_apellido)
    
    logger.info(f"🔍 Reconciliando nombres:")
    logger.info(f"   Texto OCR: '{texto_name}'")
    logger.info(f"   Query nombre: '{query_nombre}' (conf: {conf_nombre})")
    logger.info(f"   Query apellido: '{query_apellido}' (conf: {conf_apellido})")
    
    # Caso 1: Si tenemos nombre Y apellido separados en queries con buena confianza
    if query_nombre and query_apellido and conf_nombre >= 50 and conf_apellido >= 50:
        # Verificar que no sean duplicados
        if not names_are_duplicated(query_nombre, query_apellido):
            combined = f"{query_nombre} {query_apellido}".strip()
            logger.info(f"✅ Usando combinación de queries: '{combined}'")
            return combined
        else:
            logger.warning("⚠️ Nombres duplicados en queries, usando solo nombre")
            if len(query_nombre) > len(query_apellido):
                return query_nombre
            else:
                return query_apellido
    
    # Caso 2: Solo tenemos nombre completo de query con buena confianza
    if query_nombre and conf_nombre >= 70:
        # Comparar longitud con texto extraído
        if not texto_name or len(query_nombre.split()) >= len(texto_name.split()):
            logger.info(f"✅ Usando nombre de query (más completo): '{query_nombre}'")
            return query_nombre
    
    # Caso 3: El texto extraído es más completo que las queries
    if texto_name:
        texto_words = len(texto_name.split())
        query_words = len(query_nombre.split()) if query_nombre else 0
        
        if texto_words > query_words and texto_words >= 3:
            logger.info(f"✅ Usando nombre del texto (más completo): '{texto_name}'")
            return texto_name
    
    # Caso 4: Fallback - usar el que tengamos disponible
    if query_nombre and conf_nombre >= 50:
        logger.info(f"✅ Fallback a query nombre: '{query_nombre}'")
        return query_nombre
    
    if texto_name:
        logger.info(f"✅ Fallback a texto: '{texto_name}'")
        return texto_name
    
    logger.warning("❌ No se pudo determinar nombre completo")
    return None


def reconcile_simple_field(texto_value, query_value, field_name, validator=None):
    """
    Reconcilia un campo simple priorizando el valor más confiable.
    """
    
    # Limpiar valores
    texto_clean = texto_value.strip() if texto_value else None
    query_clean = query_value.strip() if query_value else None
    
    # Si tenemos validador, usarlo
    if validator:
        texto_valid = validator(texto_clean) if texto_clean else False
        query_valid = validator(query_clean) if query_clean else False
        
        if query_valid and not texto_valid:
            logger.info(f"🔄 {field_name}: Query válido '{query_clean}' vs texto inválido '{texto_clean}'")
            return query_clean
        elif texto_valid and not query_valid:
            logger.info(f"🔄 {field_name}: Texto válido '{texto_clean}' vs query inválido '{query_clean}'")
            return texto_clean
        elif query_valid and texto_valid:
            # Ambos válidos, preferir query si no son iguales
            if query_clean != texto_clean:
                logger.info(f"🔄 {field_name}: Ambos válidos, prefiriendo query '{query_clean}'")
                return query_clean
    
    # Sin validador o ambos válidos iguales
    if query_clean and query_clean not in ['NC', 'N/A', 'NO APLICA']:
        return query_clean
    
    return texto_clean


def reconcile_date_field(texto_date, query_date, field_name):
    """
    Reconcilia campos de fecha priorizando el formato más válido.
    """
    
    # Normalizar ambas fechas
    texto_normalized = normalize_date_improved(texto_date) if texto_date else None
    query_normalized = normalize_date_improved(query_date) if query_date else None
    
    logger.info(f"📅 Reconciliando {field_name}:")
    logger.info(f"   Texto: '{texto_date}' → '{texto_normalized}'")
    logger.info(f"   Query: '{query_date}' → '{query_normalized}'")
    
    # Priorizar la fecha que se pudo normalizar correctamente
    if query_normalized and not texto_normalized:
        logger.info(f"✅ Usando fecha de query (normalizada correctamente)")
        return query_normalized
    elif texto_normalized and not query_normalized:
        logger.info(f"✅ Usando fecha de texto (normalizada correctamente)")
        return texto_normalized
    elif query_normalized and texto_normalized:
        # Ambas válidas, comparar si son la misma fecha
        if query_normalized == texto_normalized:
            return query_normalized
        else:
            # Diferentes fechas válidas, preferir query
            logger.info(f"⚠️ Fechas diferentes pero válidas, prefiriendo query")
            return query_normalized
    
    # Ninguna se pudo normalizar, retornar la original que tengamos
    return query_date or texto_date


def clean_name_value(name):
    """Limpia un valor de nombre"""
    if not name:
        return None
    
    # Convertir a string y limpiar
    clean = str(name).strip()
    
    # Remover caracteres no deseados pero mantener acentos
    clean = re.sub(r'[^\w\s\-\']', '', clean)
    
    # Normalizar espacios
    clean = re.sub(r'\s+', ' ', clean)
    
    # Verificar que tiene contenido válido
    if len(clean) < 2 or clean.isdigit():
        return None
    
    return clean


def names_are_duplicated(nombre, apellido):
    """
    Verifica si nombre y apellido contienen información duplicada.
    """
    if not nombre or not apellido:
        return False
    
    nombre_words = set(nombre.upper().split())
    apellido_words = set(apellido.upper().split())
    
    # Si hay intersección significativa, son duplicados
    intersection = nombre_words.intersection(apellido_words)
    
    # Si más del 50% de las palabras se repiten, considerarlo duplicado
    min_words = min(len(nombre_words), len(apellido_words))
    if min_words > 0 and len(intersection) / min_words > 0.5:
        return True
    
    return False


def split_full_name(full_name):
    """
    Intenta separar un nombre completo en nombres y apellidos.
    Asume que los primeros 1-2 elementos son nombres y el resto apellidos.
    """
    if not full_name:
        return None
    
    parts = full_name.strip().split()
    
    if len(parts) <= 1:
        return {'nombres': full_name, 'apellidos': None}
    elif len(parts) == 2:
        return {'nombres': parts[0], 'apellidos': parts[1]}
    elif len(parts) == 3:
        return {'nombres': parts[0], 'apellidos': ' '.join(parts[1:])}
    else:
        # 4 o más partes, asumir primeros 2 son nombres
        return {'nombres': ' '.join(parts[:2]), 'apellidos': ' '.join(parts[2:])}


def extract_id_from_text_or_entities(text):
    """
    Intenta extraer número de identificación del texto o entidades como último recurso.
    """
    if not text:
        return None
    
    # Patrones para diferentes tipos de ID
    patterns = [
        r'\b(\d{1,2}-\d{3,4}-\d{1,4})\b',  # Cédula panameña
        r'\b(\d{8}[A-Z])\b',               # DNI español
        r'\b([A-Z]{2}\d{7})\b',            # Pasaporte
    ]
    
    for pattern in patterns:
        match = re.search(pattern, text)
        if match:
            return match.group(1)
    
    return None


def log_reconciliation_changes(original_data, reconciled_data):
    """
    Registra los cambios realizados durante la reconciliación.
    """
    changes = []
    
    key_fields = ['nombre_completo', 'genero', 'fecha_nacimiento', 'fecha_emision', 'lugar_nacimiento']
    
    for field in key_fields:
        original_val = original_data.get(field)
        reconciled_val = reconciled_data.get(field)
        
        if original_val != reconciled_val:
            changes.append({
                'field': field,
                'original': original_val,
                'reconciled': reconciled_val
            })
    
    if changes:
        logger.info("🔄 Cambios realizados durante reconciliación:")
        for change in changes:
            logger.info(f"   {change['field']}: '{change['original']}' → '{change['reconciled']}'")
    else:
        logger.info("✅ No se requirieron cambios durante la reconciliación")

def extract_id_document_data_improved_with_reconciliation(text, entidades=None, metadatos=None):
    """
    Versión mejorada que incluye reconciliación con query_answers.
    """
    
    # 1. Extraer datos usando el método existente
    logger.info("🔍 Extrayendo datos del texto OCR...")
    extracted_data = extract_id_document_data_improved_core(text, entidades, metadatos)
    
    # 2. Reconciliar con query_answers si están disponibles
    if metadatos:
        logger.info("🔄 Iniciando reconciliación con query_answers...")
        extracted_data = reconcile_identity_data(extracted_data, metadatos)
    else:
        logger.info("ℹ️ No hay metadatos disponibles para reconciliación")
    
    return extracted_data

def validate_cedula_with_queries(id_data, metadatos):
    """
    Validación adicional específica para cédulas usando query_answers.
    """
    
    try:
        # Obtener query_answers
        if isinstance(metadatos, str):
            metadatos_dict = json.loads(metadatos)
        else:
            metadatos_dict = metadatos
        
        query_answers = metadatos_dict.get('query_answers', {})
        
        if not query_answers:
            return id_data
        
        logger.info("🔍 Validando cédula con query_answers...")
        
        # Verificar coherencia de fechas
        fecha_nac_query = query_answers.get('fecha_nacimiento', {}).get('answer')
        fecha_exp_query = query_answers.get('fecha_expedicion', {}).get('answer')
        
        if fecha_nac_query and fecha_exp_query:
            # Asegurar que la fecha de expedición es posterior al nacimiento
            nac_norm = normalize_date_improved(fecha_nac_query)
            exp_norm = normalize_date_improved(fecha_exp_query)
            
            if nac_norm and exp_norm:
                try:
                    from datetime import datetime
                    nac_dt = datetime.strptime(nac_norm, '%Y-%m-%d')
                    exp_dt = datetime.strptime(exp_norm, '%Y-%m-%d')
                    
                    if exp_dt <= nac_dt:
                        logger.warning(f"⚠️ Fecha de expedición incoherente: {exp_norm} <= {nac_norm}")
                        # Mantener solo la fecha de nacimiento que suele ser más confiable
                        id_data['fecha_emision'] = None
                except ValueError:
                    logger.warning("Error validando coherencia de fechas")
        
        # Verificar que el número de cédula esté presente
        if not id_data.get('numero_identificacion'):
            # Intentar extraer de entidades detectadas o texto
            numero_inferido = extract_cedula_number_from_entities_or_text(
                metadatos_dict.get('entidades_detectadas', {}),
                id_data.get('texto_completo', '')
            )
            if numero_inferido:
                id_data['numero_identificacion'] = numero_inferido
                logger.info(f"📝 Número de cédula inferido: {numero_inferido}")
        
        # Asegurar país de emisión para cédulas panameñas
        if not id_data.get('pais_emision'):
            id_data['pais_emision'] = 'Panamá'
            logger.info("🌍 País de emisión asignado: Panamá")
        
        return id_data
        
    except Exception as e:
        logger.error(f"Error en validación de cédula: {str(e)}")
        return id_data


def extract_cedula_number_from_entities_or_text(entidades, texto):
    """
    Intenta extraer número de cédula de entidades detectadas o texto.
    """
    
    # Primero intentar con entidades detectadas
    if isinstance(entidades, dict):
        phone_numbers = entidades.get('phone', [])
        for phone in phone_numbers:
            if re.match(r'^\d{1,2}-\d{3,4}-\d{1,4}$', phone):
                logger.info(f"📱 Número de cédula encontrado en entidades: {phone}")
                return phone
    
    # Si no se encuentra en entidades, buscar en texto
    if texto:
        cedula_match = re.search(r'\b(\d{1,2}-\d{3,4}-\d{1,4})\b', texto)
        if cedula_match:
            logger.info(f"📄 Número de cédula encontrado en texto: {cedula_match.group(1)}")
            return cedula_match.group(1)
    
    return None

def lambda_handler(event, context):
    """
    Función principal CORREGIDA para procesar documentos de identidad CON RECONCILIACIÓN
    """
    start_time = time.time()
    logger.info("=" * 80)
    logger.info("🚀 INICIANDO PROCESAMIENTO DE DOCUMENTO DE IDENTIDAD CON RECONCILIACIÓN")
    logger.info("=" * 80)
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
            'datos_extraidos': False,
            'reconciliacion_aplicada': False
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
                'procesamiento_identidad_con_reconciliacion',
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
            
            # ==================== EXTRACCIÓN CON RECONCILIACIÓN ====================
            
            logger.info(f"🔍 Iniciando extracción con reconciliación de datos...")
            
            entidades = document_data_result['extracted_data'].get('entidades')
            metadatos = document_data_result['extracted_data'].get('metadatos_extraccion')
            
            # Registrar sub-proceso de extracción
            sub_registro_id = log_document_processing_start(
                document_id, 
                'extraccion_datos_identidad_con_reconciliacion',
                datos_entrada={
                    "texto_longitud": len(extracted_text),
                    "tiene_metadatos": bool(metadatos),
                    "tiene_entidades": bool(entidades)
                },
                analisis_id=registro_id
            )
            
            # ✅ UNA SOLA LLAMADA - CON RECONCILIACIÓN INTEGRADA
            id_data = extract_id_document_data_improved_with_reconciliation(
                extracted_text, 
                entidades, 
                metadatos
            )
            
            tipo_detectado = id_data.get('tipo_identificacion', 'desconocido')
            documento_detalle['tipo_detectado'] = tipo_detectado
            
            # Verificar si se aplicó reconciliación
            if metadatos:
                documento_detalle['reconciliacion_aplicada'] = True
                logger.info("✅ Reconciliación con query_answers aplicada")
            
            # Validación adicional para cédulas
            if tipo_detectado in ['cedula_panama', 'cedula'] and metadatos:
                logger.info("🔍 Aplicando validación adicional para cédula...")
                id_data = validate_cedula_with_queries(id_data, metadatos)
            
            # Finalizar registro de extracción
            log_document_processing_end(
                sub_registro_id, 
                estado='completado',
                datos_procesados={
                    "tipo_detectado": tipo_detectado,
                    "numero_extraido": bool(id_data.get('numero_identificacion')),
                    "nombre_extraido": bool(id_data.get('nombre_completo')),
                    "campos_totales": len([k for k, v in id_data.items() if v is not None]),
                    "reconciliacion_aplicada": documento_detalle['reconciliacion_aplicada']
                }
            )
            
            # ==================== VALIDACIÓN MEJORADA ====================
            
            logger.info(f"✅ Validando datos extraídos...")
            validation = validate_id_document_improved(id_data)
            confidence = validation['confidence']
            
            logger.info(f"📊 Validación completada - Confianza: {confidence:.2f}")
            if validation['errors']:
                logger.error(f"❌ Errores encontrados: {'; '.join(validation['errors'])}")
            if validation['warnings']:
                logger.warning(f"⚠️ Advertencias: {'; '.join(validation['warnings'])}")
            
            # Evaluación de confianza
            requires_review = evaluate_confidence(
                confidence,
                document_type=tipo_detectado,
                validation_results=validation
            )
            
            if requires_review:
                documento_detalle['estado'] = 'requiere_revision'
                response['requieren_revision'] += 1
                logger.warning(f"⚠️ Documento {document_id} requiere revisión manual")
                
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
            
            # Criterios mejorados para guardar (más flexibles después de reconciliación)
            should_save = (
                id_data.get('numero_identificacion') and 
                not id_data['numero_identificacion'].startswith('AUTO-')
            )
            
            # Si tenemos nombre completo después de reconciliación, es un plus
            if id_data.get('nombre_completo') and id_data['nombre_completo'] != 'Titular no identificado':
                logger.info("✅ Nombre completo disponible después de reconciliación")

            if should_save:
                logger.info(f"💾 Guardando datos extraídos en base de datos...")
                
                db_registro_id = log_document_processing_start(
                    document_id, 
                    'guardar_datos_identidad',
                    datos_entrada={
                        "tipo_documento": tipo_detectado,
                        "confidence": confidence,
                        "valid": validation['is_valid'],
                        "reconciliacion_aplicada": documento_detalle['reconciliacion_aplicada']
                    },
                    analisis_id=registro_id
                )
                
                success = register_document_identification_improved(document_id, id_data)
                
                if success:
                    logger.info(f"✅ Datos guardados exitosamente")
                    documento_detalle['datos_extraidos'] = True
                    log_document_processing_end(db_registro_id, estado='completado')
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
                logger.warning(f"⚠️ Datos insuficientes para guardar")
                documento_detalle['estado'] = 'datos_insuficientes'
                response['requieren_revision'] += 1
                
                update_document_processing_status(
                    document_id, 
                    'requiere_revision_manual',
                    f"Datos extraídos insuficientes tras reconciliación. "
                    f"Número: {id_data.get('numero_identificacion')}, "
                    f"Nombre: {id_data.get('nombre_completo')}"
                )
            
            # ==================== ACTUALIZAR DOCUMENTO PRINCIPAL ====================
            
            update_id = log_document_processing_start(
                document_id, 
                'actualizar_documento_principal',
                datos_entrada={
                    "confidence": confidence, 
                    "is_valid": validation['is_valid'],
                    "reconciliacion_aplicada": documento_detalle['reconciliacion_aplicada']
                },
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
                message = "Documento procesado con reconciliación - Requiere revisión manual"
            elif validation['is_valid']:
                status = 'procesamiento_completado'
                message = "Documento de identidad procesado correctamente con reconciliación"
            else:
                status = 'requiere_revision_manual'
                message = "Documento procesado con reconciliación y advertencias"
            
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
                'datos_guardados': should_save,
                'reconciliacion_aplicada': documento_detalle['reconciliacion_aplicada']
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
            reconciliacion_msg = "✅ Aplicada" if documento_detalle['reconciliacion_aplicada'] else "❌ No disponible"
            logger.info(f"   🔄 Reconciliación: {reconciliacion_msg}")
            
        except Exception as e:
            error_msg = str(e)
            doc_id = document_id if 'document_id' in locals() else 'DESCONOCIDO'
            logger.error(f"❌ Error procesando documento {doc_id}: {error_msg}")
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
                        f"Error en procesamiento de identidad con reconciliación: {error_msg}"
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

    logger.info(f"📝 Estado base de datos 1: {response['procesados']}")

    if response['procesados'] > 0 or response['requieren_revision'] > 0:
        # Buscar CUALQUIER documento que haya sido procesado (exitoso o con revisión)
        last_processed_doc = None
        for detalle in response['detalles']:
            if detalle['estado'] in ['procesado', 'requiere_revision', 'datos_insuficientes']:
                last_processed_doc = detalle['documento_id']
                break
        
        if last_processed_doc:
            cliente_id = get_client_id_by_document(last_processed_doc)
            if cliente_id:
                logger.info(f"👤 Asignando carpeta para documento {document_id}")
                assign_folder_and_link(cliente_id, last_processed_doc)
    
            logger.info(f"📝 Estado antes de crear instancia: {status}")

            # ==================== PUBLICAR EVENTO ====================
            crear_instancia_flujo_documento(last_processed_doc)
            
            # ==================== RESUMEN FINAL ====================
            logger.info(f"📝 Estado despues de crear instancia: {status}")

            total_time = time.time() - start_time
            response['tiempo_total'] = total_time
            response['total_registros'] = len(event['Records'])
            
            logger.info("=" * 80)
            logger.info("📊 RESUMEN DEL PROCESAMIENTO CON RECONCILIACIÓN")
            logger.info("=" * 80)
            logger.info(f"✅ Documentos procesados exitosamente: {response['procesados']}")
            logger.info(f"⚠️ Documentos que requieren revisión: {response['requieren_revision']}")
            logger.info(f"❌ Documentos con errores: {response['errores']}")
            logger.info(f"⏱️ Tiempo total: {total_time:.2f} segundos")
            logger.info(f"📝 Estado: {status}")
            
            # Mostrar estadísticas de reconciliación
            documentos_con_reconciliacion = sum(
                1 for d in response['detalles'] if d.get('reconciliacion_aplicada', False)
            )
            logger.info(f"🔄 Documentos con reconciliación aplicada: {documentos_con_reconciliacion}")
            
            if response['procesados'] > 0 or response['requieren_revision'] > 0:
                logger.info("🎉 Procesamiento con reconciliación completado con resultados")
            else:
                logger.warning("⚠️ Procesamiento completado SIN documentos exitosos")
            

    return {
        'statusCode': 200,
        'body': json.dumps(response, ensure_ascii=False)
    }
 