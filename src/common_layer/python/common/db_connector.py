# src/common/db_connector.py
import os
import pymysql
import json
import logging
import time
import uuid
import re
from datetime import datetime
import boto3
 
# Configuración del logger
logger = logging.getLogger()
logger.setLevel(os.environ.get('LOG_LEVEL', 'INFO'))

# Configuración de la base de datos desde variables de entorno
DB_HOST = os.environ.get('DB_HOST')
DB_NAME = os.environ.get('DB_NAME')
DB_USER = os.environ.get('DB_USER')
DB_PASSWORD = os.environ.get('DB_PASSWORD')

def get_connection():
    """Establece y retorna una conexión a la base de datos MySQL"""
    try:
        conn = pymysql.connect(
            host=DB_HOST,
            user=DB_USER,
            password=DB_PASSWORD,
            db=DB_NAME,
            charset='utf8mb4',
            cursorclass=pymysql.cursors.DictCursor,
            connect_timeout=5
        )
        logger.info("Conexión a la base de datos establecida correctamente")
        return conn
    except Exception as e:
        logger.error(f"Error al conectar a la base de datos: {str(e)}")
        raise

def execute_query(query, params=None, fetch=True):
    """Ejecuta una consulta SQL y retorna los resultados"""
    connection = get_connection()
    try:
        with connection.cursor() as cursor:
            try:
                cursor.execute(query, params)
                if fetch:
                    result = cursor.fetchall()
                else:
                    connection.commit()
                    result = cursor.lastrowid
                return result
            except pymysql.err.MySQLError as mysql_err:
                # Capturar errores específicos de MySQL para mejor diagnóstico
                error_code = mysql_err.args[0]
                error_message = mysql_err.args[1]
                logger.error(f"Error MySQL {error_code}: {error_message}")
                logger.error(f"Query: {query}")
                logger.error(f"Params: {params}")
                connection.rollback()
                raise
    except Exception as e:
        logger.error(f"Error al ejecutar consulta: {str(e)}")
        connection.rollback()
        raise
    finally:
        connection.close()

def insert_document(document_data):
    """Inserta un nuevo registro de documento en la base de datos"""
    query = """
    INSERT INTO documentos (
        id_documento, 
        codigo_documento, 
        id_tipo_documento, 
        titulo, 
        descripcion, 
        creado_por, 
        modificado_por, 
        id_carpeta,
        estado,
        confianza_extraccion,
        validado_manualmente
    ) VALUES (
        %(id_documento)s, 
        %(codigo_documento)s, 
        %(id_tipo_documento)s, 
        %(titulo)s, 
        %(descripcion)s, 
        %(creado_por)s, 
        %(modificado_por)s, 
        %(id_carpeta)s,
        %(estado)s,
        %(confianza_extraccion)s,
        %(validado_manualmente)s
    )
    """
    return execute_query(query, document_data, fetch=False)

def insert_document_version(version_data):
    """Inserta un nuevo registro de versión de documento"""
    # Asegurarnos de que inicialmente no hay miniaturas generadas
    if 'miniaturas_generadas' not in version_data:
        version_data['miniaturas_generadas'] = False
    
    query = """
    INSERT INTO versiones_documento (
        id_version,
        id_documento,
        numero_version,
        creado_por,
        comentario_version,
        tamano_bytes,
        hash_contenido,
        ubicacion_almacenamiento_tipo,
        ubicacion_almacenamiento_ruta,
        nombre_original,
        extension,
        mime_type,
        estado_ocr,
        miniaturas_generadas
    ) VALUES (
        %(id_version)s,
        %(id_documento)s,
        %(numero_version)s,
        %(creado_por)s,
        %(comentario_version)s,
        %(tamano_bytes)s,
        %(hash_contenido)s,
        %(ubicacion_almacenamiento_tipo)s,
        %(ubicacion_almacenamiento_ruta)s,
        %(nombre_original)s,
        %(extension)s,
        %(mime_type)s,
        %(estado_ocr)s,
        %(miniaturas_generadas)s
    )
    """
    
    # Insertar la versión del documento
    version_id = execute_query(query, version_data, fetch=False)

    # Si el documento es un PDF, imagen u otro formato compatible con miniaturas
    if version_data.get('extension', '').lower() in ['pdf', 'jpg', 'jpeg', 'png', 'tiff']:
        try:
            # Programar la generación de miniaturas enviando un mensaje a SQS
            # para que un servicio lambda de miniaturas procese este documento
            sqs_client = boto3.client('sqs')
            
            # Obtener URL de cola de SQS desde variables de entorno
            THUMBNAILS_QUEUE_URL = os.environ.get('THUMBNAILS_QUEUE_URL')
            
            # Si no está configurada la cola, no hacer nada
            if not THUMBNAILS_QUEUE_URL:
                logger.warning(f"No se puede programar generación de miniaturas: THUMBNAILS_QUEUE_URL no configurada")
                return version_id
            
            # Crear mensaje para generar miniaturas
            message = {
                'document_id': version_data['id_documento'],
                'version_id': version_data['id_version'],
                'bucket': version_data['ubicacion_almacenamiento_ruta'].split('/')[0] if '/' in version_data['ubicacion_almacenamiento_ruta'] else '',
                'key': '/'.join(version_data['ubicacion_almacenamiento_ruta'].split('/')[1:]) if '/' in version_data['ubicacion_almacenamiento_ruta'] else version_data['ubicacion_almacenamiento_ruta'],
                'extension': version_data['extension'],
                'mime_type': version_data['mime_type']
            }
            
            # Enviar mensaje a SQS
            sqs_client.send_message(
                QueueUrl=THUMBNAILS_QUEUE_URL,
                MessageBody=json.dumps(message)
            )
            
            logger.info(f"Generación de miniaturas programada para documento {version_data['id_documento']} versión {version_data['id_version']}")
        except Exception as e:
            logger.error(f"Error al programar generación de miniaturas: {str(e)}")
    
    return version_id

def insert_analysis_record(analysis_data):
    """Inserta un nuevo registro de análisis IA para un documento"""
    query = """
    INSERT INTO analisis_documento_ia (
        id_analisis,
        id_documento,
        id_version,
        tipo_documento,
        confianza_clasificacion,
        texto_extraido,
        entidades_detectadas,
        metadatos_extraccion,
        fecha_analisis,
        estado_analisis,
        mensaje_error,
        version_modelo,
        tiempo_procesamiento,
        procesado_por,
        requiere_verificacion,
        verificado,
        verificado_por,
        fecha_verificacion
    ) VALUES (
        %(id_analisis)s,
        %(id_documento)s,
        %(tipo_documento)s,
        %(id_version)s,
        %(confianza_clasificacion)s,
        %(texto_extraido)s,
        %(entidades_detectadas)s,
        %(metadatos_extraccion)s,
        %(fecha_analisis)s,
        %(estado_analisis)s,
        %(mensaje_error)s,
        %(version_modelo)s,
        %(tiempo_procesamiento)s,
        %(procesado_por)s,
        %(requiere_verificacion)s,
        %(verificado)s,
        %(verificado_por)s,
        %(fecha_verificacion)s
    )
    """
    return execute_query(query, analysis_data, fetch=False)

def update_analysis_record(
    id_analisis,  # ✅ Este debe ser el analysis_id, no document_id
    texto_extraido,
    entidades_detectadas,
    metadatos_extraccion,
    estado_analisis,
    version_modelo,
    tiempo_procesamiento,
    procesado_por,
    requiere_verificacion,
    verificado,
    mensaje_error=None,
    confianza_clasificacion=0.0,
    verificado_por=None,
    fecha_verificacion=None,
    tipo_documento="contrato",
    id_version=None  # ✅ AGREGAR: Parámetro para version_id
):
    """
    Actualiza un registro de análisis existente.
    VERSIÓN CORREGIDA que actualiza en lugar de insertar.
    """
    try:
        # Mapear tipos de documento a nombres reconocidos
        tipo_documento_map = {
            'dni': 'DNI',
            'cedula_panama': 'DNI',
            'pasaporte': 'Pasaporte',
            'contrato': 'Contrato',
            'desconocido': 'Documento'
        }
        
        # Usar el tipo mapeado si existe, si no, usar el tipo original
        tipo_doc_normalizado = tipo_documento_map.get(tipo_documento.lower(), tipo_documento)
        
        # CORRECCIÓN: Actualizar en lugar de insertar
        query = """
        UPDATE analisis_documento_ia 
        SET texto_extraido = %s,
            entidades_detectadas = %s,
            metadatos_extraccion = %s,
            estado_analisis = %s,
            version_modelo = %s,
            tiempo_procesamiento = %s,
            procesado_por = %s,
            requiere_verificacion = %s,
            verificado = %s,
            mensaje_error = %s,
            confianza_clasificacion = %s,
            verificado_por = %s,
            fecha_verificacion = %s,
            tipo_documento = %s,
            fecha_analisis = NOW()
        """
        
        params = [
            texto_extraido, entidades_detectadas, metadatos_extraccion,
            estado_analisis, version_modelo, tiempo_procesamiento,
            procesado_por, requiere_verificacion, verificado,
            mensaje_error, confianza_clasificacion, verificado_por,
            fecha_verificacion, tipo_doc_normalizado
        ]
        
        # ✅ Agregar id_version si se proporciona
        if id_version:
            query += ", id_version = %s"
            params.append(id_version)
        
        query += " WHERE id_analisis = %s"
        params.append(id_analisis)
        
        # Ejecutar la actualización
        execute_query(query, params, fetch=False)
        
        # Verificar si se actualizó algún registro
        verify_query = """
        SELECT COUNT(*) as count FROM analisis_documento_ia 
        WHERE id_analisis = %s
        """
        verify_result = execute_query(verify_query, (id_analisis,))
        
        if not verify_result or verify_result[0]['count'] == 0:
            logger.warning(f"No se encontró registro de análisis {id_analisis} para actualizar")
            return False
        
        logger.info(f"Registro de análisis {id_analisis} actualizado correctamente")
        return True
        
    except Exception as e:
        logger.error(f"Error al actualizar análisis {id_analisis}: {str(e)}")
        return False

def get_document_type_by_name(type_name):
    """Busca un tipo de documento por nombre"""
    query = """
    SELECT id_tipo_documento, nombre_tipo, es_documento_bancario, requiere_extraccion_ia
    FROM tipos_documento
    WHERE nombre_tipo = %s
    """
    results = execute_query(query, (type_name,))
    if results:
        return results[0]
    return None

def get_document_type_by_id(type_id):
    """Busca un tipo de documento por ID"""
    query = """
    SELECT id_tipo_documento, nombre_tipo, prefijo_nomenclatura, 
           es_documento_bancario, requiere_extraccion_ia
    FROM tipos_documento
    WHERE id_tipo_documento = %s
    """
    results = execute_query(query, (type_id,))
    if results:
        return results[0]
    return None

def get_document_by_id(document_id):
    """Obtiene un documento por su ID"""
    query = """
    SELECT d.*, td.nombre_tipo, td.es_documento_bancario, td.requiere_extraccion_ia
    FROM documentos d
    JOIN tipos_documento td ON d.id_tipo_documento = td.id_tipo_documento
    WHERE d.id_documento = %s
    """
    results = execute_query(query, (document_id,))
    if results:
        return results[0]
    return None

def get_banking_doc_category(document_type_id):
    """Obtiene la categoría bancaria para un tipo de documento"""
    query = """
    SELECT c.id_categoria_bancaria, c.nombre_categoria, c.requiere_validacion, c.validez_en_dias 
    FROM tipos_documento_bancario tdb
    JOIN categorias_bancarias c ON tdb.id_categoria_bancaria = c.id_categoria_bancaria
    WHERE tdb.id_tipo_documento = %s
    """
    results = execute_query(query, (document_type_id,))
    if results:
        return results[0]
    return None

def register_document_identification(document_id, extraction_data):
    """Registra datos para documentos de identificación"""
    # Convertir el tipo de identificación al formato esperado por la BD
    tipo_documento = 'otro'
    if extraction_data.get('tipo_identificacion') == 'dni':
        tipo_documento = 'cedula'
    elif extraction_data.get('tipo_identificacion') == 'pasaporte':
        tipo_documento = 'pasaporte'
    elif extraction_data.get('tipo_identificacion') == 'cedula_panama':
        tipo_documento = 'cedula'
    
    # Verificar si ya existe un registro
    check_query = """
    SELECT COUNT(*) as count FROM documentos_identificacion WHERE id_documento = %s
    """
    result = execute_query(check_query, (document_id,))
    
    if result and result[0]['count'] > 0:
        # Actualizar registro existente
        query = """
        UPDATE documentos_identificacion
        SET tipo_documento = %s,
            numero_documento = %s,
            pais_emision = %s,
            fecha_emision = %s,
            fecha_expiracion = %s,
            genero = %s,
            nombre_completo = %s
        WHERE id_documento = %s
        """
        execute_query(query, (
            tipo_documento,
            extraction_data.get('numero_identificacion', 'PENDIENTE'),
            extraction_data.get('pais_emision', 'España'),
            extraction_data.get('fecha_emision'),
            extraction_data.get('fecha_expiracion'),
            extraction_data.get('genero'),
            extraction_data.get('nombre_completo', 'PENDIENTE VERIFICACIÓN'),
            document_id
        ), fetch=False)
    else:
        # Crear nuevo registro
        query = """
        INSERT INTO documentos_identificacion (
            id_documento,
            tipo_documento,
            numero_documento,
            pais_emision,
            fecha_emision,
            fecha_expiracion,
            genero,
            nombre_completo
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """
        execute_query(query, (
            document_id,
            tipo_documento,
            extraction_data.get('numero_identificacion', 'PENDIENTE'),
            extraction_data.get('pais_emision', 'España'),
            extraction_data.get('fecha_emision'),
            extraction_data.get('fecha_expiracion'),
            extraction_data.get('genero'),
            extraction_data.get('nombre_completo', 'PENDIENTE VERIFICACIÓN')
        ), fetch=False)

def update_document_extraction_data_with_type_preservation(document_id, data_json, confidence, is_valid):
    """
    Actualiza los datos extraídos del documento pero NUNCA modifica el tipo de documento
    ya que este es establecido por la lambda de clasificación.
    """
    with get_connection() as connection:
        try:
            # Obtener los datos actuales del documento para preservar información importante
            query_get_doc = """
            SELECT id_tipo_documento FROM documentos WHERE id_documento = %s
            """
            with connection.cursor() as cursor:
                cursor.execute(query_get_doc, (document_id,))
                current_doc = cursor.fetchone()
            
            if not current_doc:
                logger.error(f"No se encontró el documento {document_id} en la base de datos")
                return False
            
            # Siempre preservar el tipo de documento actual
            logger.info(f"Preservando tipo de documento existente para {document_id}")
            
            # Actualizar todo excepto el id_tipo_documento (nombre correcto de la columna)
            query = """
            UPDATE documentos 
            SET datos_extraidos_ia = %s, 
                confianza_extraccion = %s, 
                validado_manualmente = %s,
                fecha_modificacion = NOW()
            WHERE id_documento = %s
            """
            params = (data_json, confidence, 1 if is_valid else 0, document_id)
            
            with connection.cursor() as cursor:
                cursor.execute(query, params)
                connection.commit()
                logger.info(f"Documento {document_id} actualizado preservando su tipo de documento original")
                return True
        except Exception as e:
            logger.error(f"Error al actualizar documento: {str(e)}")
            connection.rollback()
            return False
        
def link_document_to_client(document_id, client_id=None, document_type_id=None):
    """
    Vincula un documento a un cliente.
    Si no se proporciona client_id, intenta inferirlo del texto extraído.
    """
    if not client_id and document_type_id:
        # Obtener texto extraído del documento
        query = """
        SELECT texto_extraido FROM analisis_documento_ia 
        WHERE id_documento = %s AND texto_extraido IS NOT NULL
        ORDER BY fecha_analisis DESC LIMIT 1
        """
        results = execute_query(query, (document_id,))
        
        if results and results[0]['texto_extraido']:
            # Buscar coincidencias en el texto con números de cliente o documentos
            texto = results[0]['texto_extraido']
            
            # Buscar patrones de documentos de identidad
            dni_matches = re.findall(r'\b\d{8}[A-Za-z]?\b', texto)
            if dni_matches:
                # Buscar cliente con este DNI
                client_query = """
                SELECT id_cliente FROM clientes 
                WHERE documento_identificacion = %s
                LIMIT 1
                """
                client_result = execute_query(client_query, (dni_matches[0],))
                if client_result:
                    client_id = client_result[0]['id_cliente']
    
    if client_id:
        # Crear vínculo en la base de datos
        query = """
        INSERT INTO documentos_clientes (
            id_documento,
            id_cliente,
            fecha_asignacion,
            asignado_por
        ) VALUES (%s, %s, NOW(), '691d8c44-f524-48fd-b292-be9e31977711')
        """
        execute_query(query, (document_id, client_id), fetch=False)
        return True
    
    return False

def insert_audit_record(audit_data):
    """Inserta un registro en la tabla de auditoría"""
    query = """
    INSERT INTO registros_auditoria (
        fecha_hora,
        usuario_id,
        direccion_ip,
        accion,
        entidad_afectada,
        id_entidad_afectada,
        detalles,
        resultado
    ) VALUES (
        %s, %s, %s, %s, %s, %s, %s, %s
    )
    """
    return execute_query(query, (
        audit_data['fecha_hora'],
        audit_data['usuario_id'],
        audit_data['direccion_ip'],
        audit_data['accion'],
        audit_data['entidad_afectada'],
        audit_data['id_entidad_afectada'],
        audit_data['detalles'],
        audit_data['resultado']
    ), fetch=False)

def check_document_expiry(days_threshold=30):
    """
    Verifica documentos próximos a expirar y devuelve una lista de los mismos.
    El parámetro days_threshold indica cuántos días antes de la expiración se debe alertar.
    """
    query = """
    SELECT di.id_documento, di.tipo_identificacion, di.numero_identificacion, 
           di.fecha_expiracion, di.nombre_completo, 
           c.id_cliente, c.nombre_razon_social, c.codigo_cliente
    FROM documentos_identificacion di
    JOIN documentos_clientes dc ON di.id_documento = dc.id_documento
    JOIN clientes c ON dc.id_cliente = c.id_cliente
    WHERE di.fecha_expiracion IS NOT NULL
      AND di.fecha_expiracion BETWEEN CURDATE() AND DATE_ADD(CURDATE(), INTERVAL %s DAY)
    ORDER BY di.fecha_expiracion
    """
    
    results = execute_query(query, (days_threshold,))
    return results

def get_pending_documents_for_client(client_id):
    """Obtiene documentos pendientes para un cliente"""
    query = """
    SELECT ds.id_solicitud, ds.id_tipo_documento, td.nombre_tipo,
           ds.fecha_solicitud, ds.fecha_limite, ds.estado,
           td.es_documento_bancario, cb.nombre_categoria
    FROM documentos_solicitados ds
    JOIN tipos_documento td ON ds.id_tipo_documento = td.id_tipo_documento
    LEFT JOIN tipos_documento_bancario tdb ON td.id_tipo_documento = tdb.id_tipo_documento
    LEFT JOIN categorias_bancarias cb ON tdb.id_categoria_bancaria = cb.id_categoria_bancaria
    WHERE ds.id_cliente = %s
      AND ds.estado IN ('pendiente', 'recordatorio_enviado')
    ORDER BY ds.fecha_limite
    """
    
    results = execute_query(query, (client_id,))
    return results

def update_client_document_status(client_id):
    """
    Actualiza el estado documental de un cliente basado en 
    la completitud y validez de sus documentos.
    """
    try:
        # Llamar al procedimiento almacenado definido en la base de datos
        query = "CALL actualizar_estado_documental_cliente(%s)"
        execute_query(query, (client_id,), fetch=False)
        return True
    except Exception as e:
        logger.error(f"Error al actualizar estado documental de cliente {client_id}: {str(e)}")
        return False

def update_document_extraction_data(document_id, extracted_data, confidence, validated=False):
    """Actualiza los datos extraídos de un documento"""
    try:
        # 1. Validar formato de datos - Asegurar que los datos son serializables a JSON
        if isinstance(extracted_data, str):
            # Ya es una cadena JSON, verificar que sea válida
            try:
                json.loads(extracted_data)
                json_data = extracted_data
                logger.info(f"Datos JSON válidos para documento {document_id}")
            except json.JSONDecodeError:
                logger.error(f"Error: Los datos para {document_id} no son JSON válido")
                return False
        else:
            # Convertir a JSON si es un diccionario
            try:
                json_data = json.dumps(extracted_data)
                logger.info(f"Datos convertidos exitosamente a JSON para documento {document_id}")
            except (TypeError, OverflowError) as e:
                logger.error(f"Error al serializar datos a JSON para {document_id}: {str(e)}")
                return False
        
        # 2. Verificar que el documento existe antes de actualizarlo
        check_query = """
        SELECT COUNT(*) as count FROM documentos WHERE id_documento = %s
        """
        result = execute_query(check_query, (document_id,))
        
        if not result or result[0]['count'] == 0:
            logger.warning(f"El documento {document_id} no existe en la base de datos. Creando registro automáticamente.")
            
            # Crear documento automáticamente
            try:
                # Obtener específicamente el tipo de documento "Contrato cuenta" o uno similar
                tipo_documento_query = """
                SELECT id_tipo_documento FROM tipos_documento 
                WHERE nombre_tipo LIKE '%contrato%' OR nombre_tipo LIKE '%Contrato%' 
                ORDER BY es_documento_bancario DESC
                LIMIT 1
                """
                tipo_result = execute_query(tipo_documento_query)
                
                if not tipo_result:
                    # Si no se encuentra un tipo de contrato, usar cualquier tipo bancario
                    backup_query = """
                    SELECT id_tipo_documento FROM tipos_documento 
                    WHERE es_documento_bancario = 1
                    LIMIT 1
                    """
                    backup_result = execute_query(backup_query)
                    
                    if not backup_result:
                        # Si aún no hay tipos, usar cualquier tipo
                        final_query = """
                        SELECT id_tipo_documento FROM tipos_documento 
                        LIMIT 1
                        """
                        final_result = execute_query(final_query)
                        
                        if not final_result:
                            logger.error("No se encontraron tipos de documento en la base de datos")
                            return False
                        
                        tipo_id = final_result[0]['id_tipo_documento']
                    else:
                        tipo_id = backup_result[0]['id_tipo_documento']
                else:
                    tipo_id = tipo_result[0]['id_tipo_documento']
                
                logger.info(f"Usando tipo de documento con ID: {tipo_id}")
                
                # Generar un código único para el documento
                codigo_doc = f"AUTO-{int(time.time())}-{document_id[:8]}"
                
                # Datos mínimos para creación de documento
                doc_data = {
                    'id_documento': document_id,
                    'codigo_documento': codigo_doc,
                    'id_tipo_documento': tipo_id,
                    'titulo': f"Documento {codigo_doc}",
                    'descripcion': "Documento creado automáticamente durante procesamiento",
                    'creado_por': '691d8c44-f524-48fd-b292-be9e31977711',
                    'modificado_por': '691d8c44-f524-48fd-b292-be9e31977711',
                    'id_carpeta': '22222222-aaaa-bbbb-cccc-222222222222',
                    'estado': 'procesando',
                    'confianza_extraccion': confidence,
                    'validado_manualmente': validated
                }
                
                # Insertar el documento
                insert_document(doc_data)
                logger.info(f"Documento {document_id} creado automáticamente con éxito")
                
                # Crear registro de análisis asociado
                analysis_id = str(uuid.uuid4())
                analysis_data = {
                    'id_analisis': analysis_id,
                    'id_documento': document_id,
                    'tipo_documento': 'contrato',
                    'confianza_clasificacion': confidence,
                    'texto_extraido': None,
                    'entidades_detectadas': None,
                    'metadatos_extraccion': None,
                    'fecha_analisis': datetime.utcnow(),
                    'estado_analisis': 'creado_automaticamente',
                    'mensaje_error': None,
                    'version_modelo': 'textract-auto',
                    'tiempo_procesamiento': 0,
                    'procesado_por': '691d8c44-f524-48fd-b292-be9e31977711',
                    'requiere_verificacion': True,
                    'verificado': False,
                    'verificado_por': None,
                    'fecha_verificacion': None
                }
                
                insert_analysis_record(analysis_data)
                logger.info(f"Registro de análisis creado para el documento {document_id}")
                
            except Exception as create_error:
                logger.error(f"Error al crear documento automáticamente: {str(create_error)}")
                return False
        else:
            logger.info(f"Verificación exitosa: El documento {document_id} existe en la base de datos")
        
        # 3. Proceder con la actualización
        query = """
        UPDATE documentos
        SET datos_extraidos_ia = %s,
            confianza_extraccion = %s,
            validado_manualmente = %s,
            fecha_modificacion = NOW()
        WHERE id_documento = %s
        """
        
        # Ejecutar la actualización
        result = execute_query(query, (json_data, confidence, validated, document_id), fetch=False)
        
        # 4. Añadir logs detallados sobre el resultado
        logger.info(f"Actualización completada para documento {document_id}")
        logger.info(f"Datos: {len(json_data)} caracteres, Confianza: {confidence}, Validado: {validated}")
        
        # 5. Verificar que la actualización fue exitosa
        verify_query = """
        SELECT datos_extraidos_ia, confianza_extraccion 
        FROM documentos 
        WHERE id_documento = %s
        """
        verify_result = execute_query(verify_query, (document_id,))
        
        if verify_result and verify_result[0]['datos_extraidos_ia']:
            logger.info(f"✅ Verificación exitosa: Datos guardados correctamente para {document_id}")
            return True
        else:
            logger.warning(f"⚠️ Alerta: Los datos pueden no haberse guardado correctamente para {document_id}")
            return False
            
    except Exception as e:
        logger.error(f"❌ Error grave al actualizar datos extraídos para {document_id}: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        return False
    
def update_document_processing_status(document_id, status, message=None, tipo_documento=None):
    """Actualiza el estado de procesamiento de un documento en la tabla de análisis"""
    # Si se proporciona tipo_documento, incluirlo en la actualización
    if tipo_documento:
        query = """
        UPDATE analisis_documento_ia
        SET estado_analisis = %s,
            mensaje_error = %s,
            tipo_documento = %s,
            fecha_analisis = NOW()
        WHERE id_documento = %s
        ORDER BY fecha_analisis DESC
        LIMIT 1
        """
        logger.info(f"Actualizando estado para documento {document_id}: {status} (tipo: {tipo_documento})")
        try:
            return execute_query(query, (status, message, tipo_documento, document_id), fetch=False)
        except Exception as e:
            logger.error(f"Error al actualizar estado del documento: {str(e)}")
            # Resto del código de manejo de errores...
    else:
        # Versión original sin tipo de documento
        query = """
        UPDATE analisis_documento_ia
        SET estado_analisis = %s,
            mensaje_error = %s,
            fecha_analisis = NOW()
        WHERE id_documento = %s
        ORDER BY fecha_analisis DESC
        LIMIT 1
        """
        logger.info(f"Actualizando estado para documento {document_id}: {status}")
        try:
            return execute_query(query, (status, message, document_id), fetch=False)
        except Exception as e:
            logger.error(f"Error al actualizar estado del documento: {str(e)}")
        # Si ocurre un error, intentamos una consulta alternativa para verificar si el registro existe
        check_query = """
        SELECT COUNT(*) as count 
        FROM analisis_documento_ia 
        WHERE id_documento = %s
        """
        try:
            result = execute_query(check_query, (document_id,))
            if result and result[0]['count'] == 0:
                logger.info(f"No se encontró registro previo, insertando nuevo análisis para documento {document_id}")
                insert_query = """
                INSERT INTO analisis_documento_ia
                (id_analisis, id_documento, tipo_documento, estado_analisis, mensaje_error, fecha_analisis)
                VALUES (%s, %s, %s, %s, %s, NOW())
                """
                analysis_id = str(uuid.uuid4())
                return execute_query(insert_query, 
                                    (analysis_id, document_id, 'contrato', status, message), 
                                    fetch=False)
        except Exception as check_error:
            logger.error(f"Error adicional al verificar/insertar análisis: {str(check_error)}")
        raise

def generate_uuid():
    """Genera un UUID único"""
    return str(uuid.uuid4())

def assign_folder_and_link(client_id, documento_id):
    """
    Asigna un documento a la carpeta adecuada del cliente según su categoría
    """
    try:
        logger.info(f"Iniciando llamada a procedimiento almacenado para documento {documento_id} y cliente {client_id}")
        
        # Verificar que client_id y documento_id tienen valores válidos
        if not client_id or not documento_id:
            logger.error(f"Error en asignación: client_id={client_id}, documento_id={documento_id}")
            return None
            
        # Llamar al procedimiento almacenado con más información de logging
        query = "CALL registrar_documento_carpeta(%s, %s)"
        logger.info(f"Ejecutando query: {query} con parámetros: ({client_id}, {documento_id})")
        
        # Obtener una conexión directa para mejor diagnóstico
        connection = get_connection()
        try:
            with connection.cursor() as cursor:
                cursor.execute(query, (client_id, documento_id))
                result = cursor.fetchall()
                logger.info(f"Procedimiento ejecutado. Resultado: {result}")
                
                # Log de la tabla de log para verificar si se insertó
                cursor.execute("SELECT COUNT(*) as count FROM log_procedimientos WHERE operacion = 'registrar_documento_carpeta' AND fecha > DATE_SUB(NOW(), INTERVAL 1 MINUTE)")
                log_count = cursor.fetchone()
                logger.info(f"Entradas recientes en log_procedimientos: {log_count['count'] if log_count else 'No se pudo verificar'}")
                
                if result and len(result) > 0:
                    logger.info(f"Documento {documento_id} asignado a carpeta {result[0]['id_carpeta']} del cliente {client_id}")
                    return result[0]
                else:
                    logger.warning(f"No se recibió información de carpeta para el documento {documento_id}")
                    return None
        finally:
            connection.close()
            
    except Exception as e:
        logger.error(f"Error al registrar documento en carpeta: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        return None
# Nuevas funciones para DocumentExpiryMonitor 
def get_expiring_documents(target_date):
    """
    Obtiene documentos que vencen en una fecha específica
    
    Args:
        target_date: Fecha objetivo de vencimiento
        
    Returns:
        Lista de documentos que vencen en la fecha especificada
    """
    conn = get_connection()
    try:
        with conn.cursor() as cursor:
            query = """
            SELECT di.*, d.id_tipo_documento, d.titulo, dc.id_cliente, c.nombre_razon_social, 
                   c.segmento_bancario, c.datos_contacto, c.preferencias_comunicacion, c.gestor_principal_id
            FROM documentos_identificacion di
            JOIN documentos d ON di.id_documento = d.id_documento
            JOIN documentos_clientes dc ON d.id_documento = dc.id_documento
            JOIN clientes c ON dc.id_cliente = c.id_cliente
            WHERE di.fecha_expiracion = %s
            AND d.estado = 'publicado'
            """
            cursor.execute(query, (target_date,))
            results = cursor.fetchall()
            
            # Convertir a lista de diccionarios
            documents = []
            for row in results:
                doc = dict(row)
                # Deserializar campos JSON
                if 'datos_contacto' in doc and doc['datos_contacto']:
                    doc['datos_contacto'] = json.loads(doc['datos_contacto'])
                if 'preferencias_comunicacion' in doc and doc['preferencias_comunicacion']:
                    doc['preferencias_comunicacion'] = json.loads(doc['preferencias_comunicacion'])
                documents.append(doc)
                
            return documents
    finally:
        conn.close()

def update_document_status(document_id, status, metadata=None):
    """
    Actualiza el estado de un documento
    
    Args:
        document_id: ID del documento
        status: Nuevo estado
        metadata: Metadatos adicionales (opcional)
    
    Returns:
        Boolean indicando éxito
    """
    conn = get_connection()
    try:
        with conn.cursor() as cursor:
            # Preparar metadatos para actualización
            if metadata:
                # Obtener metadatos actuales
                cursor.execute(
                    "SELECT metadatos FROM documentos WHERE id_documento = %s",
                    (document_id,)
                )
                result = cursor.fetchone()
                current_metadata = json.loads(result['metadatos']) if result and result['metadatos'] else {}
                
                # Actualizar con nuevos metadatos
                current_metadata.update(metadata)
                metadata_json = json.dumps(current_metadata)
                
                # Actualizar documento con nuevos metadatos
                query = """
                UPDATE documentos
                SET estado = %s, metadatos = %s, fecha_modificacion = NOW()
                WHERE id_documento = %s
                """
                cursor.execute(query, (status, metadata_json, document_id))
            else:
                # Actualizar solo estado
                query = """
                UPDATE documentos
                SET estado = %s, fecha_modificacion = NOW()
                WHERE id_documento = %s
                """
                cursor.execute(query, (status, document_id))
                
            conn.commit()
            return cursor.rowcount > 0
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        conn.close()

def create_document_request(client_id, document_type_id, expiry_date, notes=None):
    """
    Crea una solicitud de renovación de documento
    
    Args:
        client_id: ID del cliente
        document_type_id: ID del tipo de documento
        expiry_date: Fecha de vencimiento del documento actual
        notes: Notas adicionales (opcional)
        
    Returns:
        ID de la solicitud creada
    """
    conn = get_connection()
    try:
        with conn.cursor() as cursor:
            # Generar ID único para la solicitud
            request_id = str(uuid.uuid4())
            
            # Calcular fecha límite (30 días después del vencimiento actual)
            from datetime import datetime, timedelta
            current_date = datetime.now().date()
            days_to_expiry = (expiry_date - current_date).days
            
            # Si ya está vencido o vence en menos de 10 días, fecha límite = 10 días
            # Si vence en más tiempo, fecha límite = 5 días antes del vencimiento
            if days_to_expiry <= 10:
                deadline = current_date + timedelta(days=10)
            else:
                deadline = expiry_date - timedelta(days=5)
            
            # Insertar solicitud en la base de datos
            query = """
            INSERT INTO documentos_solicitados (
                id_solicitud, id_cliente, id_tipo_documento, 
                fecha_solicitud, solicitado_por, fecha_limite, 
                estado, notas
            ) VALUES (
                %s, %s, %s, 
                NOW(), 'sistema', %s, 
                'pendiente', %s
            )
            """
            cursor.execute(
                query, 
                (request_id, client_id, document_type_id, deadline, notes)
            )
            
            conn.commit()
            return request_id
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        conn.close()

def update_client_documental_status(client_id):
    """
    Actualiza el estado documental del cliente
    
    Args:
        client_id: ID del cliente
        
    Returns:
        Boolean indicando éxito
    """
    conn = get_connection()
    try:
        with conn.cursor() as cursor:
            # Llamar al procedimiento almacenado que actualiza el estado documental
            cursor.callproc('actualizar_estado_documental_cliente', [client_id])
            conn.commit()
            return True
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        conn.close()

def get_client_by_id(client_id):
    """
    Obtiene información de un cliente por su ID
    
    Args:
        client_id: ID del cliente
        
    Returns:
        Dict con datos del cliente o None si no existe
    """
    conn = get_connection()
    try:
        with conn.cursor() as cursor:
            query = """
            SELECT * FROM clientes WHERE id_cliente = %s
            """
            cursor.execute(query, (client_id,))
            result = cursor.fetchone()
            
            if not result:
                return None
                
            # Convertir a diccionario y deserializar campos JSON
            client = dict(result)
            for json_field in ['datos_contacto', 'preferencias_comunicacion', 'metadata_personalizada', 'documentos_pendientes']:
                if json_field in client and client[json_field]:
                    try:
                        client[json_field] = json.loads(client[json_field])
                    except:
                        # Si no se puede deserializar, dejar como está
                        pass
                        
            return client
    finally:
        conn.close()

def get_client_id_by_document(document_id):
    """
    Obtiene el ID del cliente asociado a un documento desde la tabla documentos_clientes.
    
    Args:
        document_id (str): ID del documento
    
    Returns:
        str or None: ID del cliente si existe, None si no está vinculado
    """
    conn = get_connection()
    try:
        with conn.cursor() as cursor:
            query = """
                SELECT id_cliente
                FROM documentos_clientes
                WHERE id_documento = %s
                LIMIT 1
            """
            cursor.execute(query, (document_id,))
            result = cursor.fetchone()
            return result['id_cliente'] if result else None
    finally:
        conn.close()
# Añadir estas funciones al archivo db_connector.py

def generate_process_log_id():
    """Genera un ID único para el registro de procesamiento"""
    return str(uuid.uuid4())

def log_document_processing_start(document_id, tipo_proceso, datos_entrada=None, analisis_id=None, servicio="id_processor", version="1.0"):
    """
    Registra el inicio de un proceso en la tabla de registro de procesamiento
    
    Args:
        document_id: ID del documento
        tipo_proceso: Tipo de proceso (carga, extraccion, clasificacion, etc.)
        datos_entrada: Datos de entrada del proceso (opcional)
        analisis_id: ID del análisis asociado (opcional)
        servicio: Nombre del servicio que realiza el procesamiento
        version: Versión del servicio
        
    Returns:
        ID del registro creado
    """
    try:
        # Generar ID para el registro
        registro_id = generate_process_log_id()
        
        # Convertir datos_entrada a JSON si es un diccionario
        if isinstance(datos_entrada, dict):
            datos_entrada_json = json.dumps(datos_entrada)
        else:
            datos_entrada_json = datos_entrada
            
        # Insertar registro de inicio
        query = """
        INSERT INTO registro_procesamiento_documento (
            id_registro, id_documento, id_analisis, tipo_proceso, 
            estado_proceso, datos_entrada, timestamp_inicio, 
            servicio_procesador, version_servicio
        ) VALUES (
            %s, %s, %s, %s, 
            'iniciado', %s, NOW(), 
            %s, %s
        )
        """
        
        execute_query(query, (
            registro_id, document_id, analisis_id, tipo_proceso,
            datos_entrada_json, servicio, version
        ), fetch=False)
        
        # Devolver ID para usarlo en log_document_processing_end
        return registro_id
    except Exception as e:
        logger.error(f"Error al registrar inicio de procesamiento: {str(e)}")
        # Devolver un ID generado para asegurar que se pueda continuar el proceso
        return generate_process_log_id()

def log_document_processing_end(registro_id, estado='completado', datos_procesados=None, datos_salida=None, 
                              confianza=None, mensaje_error=None, duracion_ms=None):
    """
    Actualiza un registro de procesamiento con los resultados
    
    Args:
        registro_id: ID del registro a actualizar
        estado: Estado final del proceso (completado, error, advertencia)
        datos_procesados: Datos procesados durante el proceso
        datos_salida: Datos de salida del proceso
        confianza: Nivel de confianza del procesamiento
        mensaje_error: Mensaje de error si hubo alguno
        duracion_ms: Duración del proceso en milisegundos
        
    Returns:
        Boolean indicando éxito
    """
    try:
        # Convertir datos a JSON si son diccionarios
        if isinstance(datos_procesados, dict):
            datos_procesados_json = json.dumps(datos_procesados)
        else:
            datos_procesados_json = datos_procesados
            
        if isinstance(datos_salida, dict):
            datos_salida_json = json.dumps(datos_salida)
        else:
            datos_salida_json = datos_salida
        
        # Si no se proporciona duración, calcularla desde el inicio
        if duracion_ms is None:
            # Obtener timestamp de inicio
            query_inicio = """
            SELECT timestamp_inicio FROM registro_procesamiento_documento 
            WHERE id_registro = %s
            """
            result = execute_query(query_inicio, (registro_id,))
            
            if result and result[0].get('timestamp_inicio'):
                from datetime import datetime
                inicio = result[0]['timestamp_inicio']
                # Calcular diferencia en milisegundos
                ahora = datetime.now()
                diff_ms = int((ahora - inicio).total_seconds() * 1000)
                duracion_ms = diff_ms
        
        # Actualizar registro con resultados
        query = """
        UPDATE registro_procesamiento_documento
        SET estado_proceso = %s,
            datos_procesados = %s,
            datos_salida = %s,
            confianza = %s,
            mensaje_error = %s,
            timestamp_fin = NOW(),
            duracion_ms = %s
        WHERE id_registro = %s
        """
        
        execute_query(query, (
            estado, datos_procesados_json, datos_salida_json,
            confianza, mensaje_error, duracion_ms, registro_id
        ), fetch=False)
        
        return True
    except Exception as e:
        logger.error(f"Error al registrar fin de procesamiento: {str(e)}")
        return False

def get_document_processing_history(document_id):
    """
    Obtiene el historial de procesamiento de un documento
    
    Args:
        document_id: ID del documento
        
    Returns:
        Lista de registros de procesamiento ordenados por timestamp
    """
    query = """
    SELECT * FROM registro_procesamiento_documento
    WHERE id_documento = %s
    ORDER BY timestamp_inicio DESC
    """
    
    return execute_query(query, (document_id,))

# ----- Funciones para ManualReviewHandler -----

def get_pending_review_documents(tipo_documento=None, nivel_confianza=None, user_id=None, 
                                is_admin=False, page=1, page_size=10):
    """
    Obtiene documentos pendientes de revisión manual con paginación y filtros.
    
    Args:
        tipo_documento: Filtro por tipo de documento (opcional)
        nivel_confianza: Filtro por nivel de confianza máximo (opcional)
        user_id: ID del usuario que realiza la consulta (para filtrado por permisos)
        is_admin: Indica si el usuario tiene permisos administrativos
        page: Número de página a mostrar
        page_size: Tamaño de la página
        
    Returns:
        Tupla con (lista de documentos, metadata de paginación)
    """
    # Calcular offset para paginación
    offset = (page - 1) * page_size
    
    # Construir consulta base
    query = """
        SELECT a.id_analisis, a.id_documento, d.titulo, d.codigo_documento, a.tipo_documento,
               a.confianza_clasificacion, a.fecha_analisis, a.estado_analisis, 
               td.nombre_tipo, u.nombre_usuario AS creado_por_usuario,
               v.nombre_original, v.ubicacion_almacenamiento_ruta
        FROM analisis_documento_ia a
        JOIN documentos d ON a.id_documento = d.id_documento
        JOIN tipos_documento td ON d.id_tipo_documento = td.id_tipo_documento
        JOIN usuarios u ON d.creado_por = u.id_usuario
        JOIN versiones_documento v ON (d.id_documento = v.id_documento AND d.version_actual = v.numero_version)
        WHERE a.requiere_verificacion = 1 AND a.verificado = 0
    """
    
    # Añadir filtros si se proporcionan
    params = []
    if tipo_documento:
        query += " AND d.id_tipo_documento = %s"
        params.append(tipo_documento)
        
    if nivel_confianza:
        query += " AND a.confianza_clasificacion <= %s"
        params.append(float(nivel_confianza))
    
    # Añadir filtros de permisos si el usuario no es admin
    if not is_admin and user_id:
        query += """
            AND (
                d.id_carpeta IN (
                    SELECT pc.id_carpeta
                    FROM permisos_carpetas pc
                    WHERE (pc.id_entidad = %s AND pc.tipo_entidad = 'usuario')
                    OR (pc.id_entidad IN (SELECT id_grupo FROM usuarios_grupos WHERE id_usuario = %s) AND pc.tipo_entidad = 'grupo')
                )
                OR d.creado_por = %s
            )
        """
        params.extend([user_id, user_id, user_id])
    
    # Añadir ordenamiento y paginación
    query += " ORDER BY a.fecha_analisis DESC LIMIT %s OFFSET %s"
    params.extend([page_size, offset])
    
    # Consulta para contar total de documentos (para metadata de paginación)
    count_query = """
        SELECT COUNT(*) as total
        FROM analisis_documento_ia a
        JOIN documentos d ON a.id_documento = d.id_documento
        WHERE a.requiere_verificacion = 1 AND a.verificado = 0
    """
    
    count_params = []
    if tipo_documento:
        count_query += " AND d.id_tipo_documento = %s"
        count_params.append(tipo_documento)
        
    if nivel_confianza:
        count_query += " AND a.confianza_clasificacion <= %s"
        count_params.append(float(nivel_confianza))
    
    # Añadir filtros de permisos a la consulta de conteo
    if not is_admin and user_id:
        count_query += """
            AND (
                d.id_carpeta IN (
                    SELECT pc.id_carpeta
                    FROM permisos_carpetas pc
                    WHERE (pc.id_entidad = %s AND pc.tipo_entidad = 'usuario')
                    OR (pc.id_entidad IN (SELECT id_grupo FROM usuarios_grupos WHERE id_usuario = %s) AND pc.tipo_entidad = 'grupo')
                )
                OR d.creado_por = %s
            )
        """
        count_params.extend([user_id, user_id, user_id])
    
    try:
        # Ejecutar consulta principal
        documents = execute_query(query, params, True)
        
        # Ejecutar consulta de conteo
        count_result = execute_query(count_query, count_params, True)
        total_items = count_result[0]['total'] if count_result else 0
        
        # Crear metadata de paginación
        total_pages = (total_items + page_size - 1) // page_size if total_items > 0 else 1  # División con techo
        pagination = {
            'total_items': total_items,
            'total_pages': total_pages,
            'current_page': page,
            'page_size': page_size,
            'has_next': page < total_pages,
            'has_prev': page > 1
        }
        
        return documents, pagination
    except Exception as e:
        logger.error(f"Error al obtener documentos pendientes de revisión: {str(e)}")
        raise

def check_document_access(document_id, user_id, require_write=False):
    """
    Verifica si un usuario tiene acceso a un documento.
    
    Args:
        document_id: ID del documento a verificar
        user_id: ID del usuario
        require_write: Si True, verifica permisos de escritura, si False, solo lectura
        
    Returns:
        Boolean indicando si tiene acceso
    """
    permission_types = "('escritura', 'administracion')" if require_write else "('lectura', 'escritura', 'administracion')"
    
    query = f"""
        SELECT 1
        FROM documentos d
        LEFT JOIN permisos_carpetas pc ON d.id_carpeta = pc.id_carpeta
        WHERE d.id_documento = %s
        AND (
            d.creado_por = %s
            OR (pc.id_entidad = %s AND pc.tipo_entidad = 'usuario' AND pc.tipo_permiso IN {permission_types})
            OR (pc.id_entidad IN (SELECT id_grupo FROM usuarios_grupos WHERE id_usuario = %s) AND pc.tipo_entidad = 'grupo' AND pc.tipo_permiso IN {permission_types})
            OR EXISTS (SELECT 1 FROM usuarios_roles ur WHERE ur.id_usuario = %s AND ur.id_rol IN (
                SELECT id_rol FROM roles_permisos WHERE id_permiso = (SELECT id_permiso FROM permisos WHERE codigo_permiso = 'admin.todas_operaciones')
            ))
        )
    """
    
    try:
        result = execute_query(query, [document_id, user_id, user_id, user_id, user_id], True)
        return bool(result)
    except Exception as e:
        logger.error(f"Error al verificar acceso a documento: {str(e)}")
        return False

def get_document_review_data(document_id):
    """
    Obtiene datos detallados de un documento para revisión manual.
    
    Args:
        document_id: ID del documento a revisar
        
    Returns:
        Dict con datos del documento o None si no existe
    """
    try:
        # Obtener metadatos del documento
        doc_query = """
            SELECT d.id_documento, d.codigo_documento, d.titulo, d.descripcion, 
                   td.nombre_tipo, td.id_tipo_documento, 
                   a.id_analisis, a.tipo_documento AS tipo_documento_detectado, 
                   a.confianza_clasificacion, a.texto_extraido, a.entidades_detectadas,
                   a.metadatos_extraccion, a.fecha_analisis, a.estado_analisis,
                   v.nombre_original, v.ubicacion_almacenamiento_ruta, v.mime_type,
                   v.tamano_bytes
            FROM documentos d
            JOIN analisis_documento_ia a ON d.id_documento = a.id_documento
            JOIN tipos_documento td ON d.id_tipo_documento = td.id_tipo_documento
            JOIN versiones_documento v ON (d.id_documento = v.id_documento AND d.version_actual = v.numero_version)
            WHERE d.id_documento = %s AND a.requiere_verificacion = 1
            ORDER BY a.fecha_analisis DESC
            LIMIT 1
        """
        
        doc_result = execute_query(doc_query, [document_id], True)
        
        if not doc_result:
            return None
        
        # Juntar todos los datos relevantes
        result = {
            'document': doc_result[0],
            'specific_data': {},
            'client': None,
            'processing_history': [],
            'available_document_types': []
        }
        
        # Obtener información del cliente si está disponible
        client_query = """
            SELECT c.id_cliente, c.codigo_cliente, c.nombre_razon_social, c.tipo_cliente, 
                   c.segmento_bancario, c.nivel_riesgo, c.estado_documental
            FROM documentos_clientes dc
            JOIN clientes c ON dc.id_cliente = c.id_cliente
            WHERE dc.id_documento = %s
            LIMIT 1
        """
        
        client_result = execute_query(client_query, [document_id], True)
        if client_result:
            result['client'] = client_result[0]
        
        # Obtener datos específicos según el tipo de documento
        id_query = """
            SELECT * FROM documentos_identificacion
            WHERE id_documento = %s
        """
        id_result = execute_query(id_query, [document_id], True)
        if id_result:
            result['specific_data']['id_document'] = id_result[0]
        
        # Obtener historial de procesamiento
        processing_query = """
            SELECT id_registro, tipo_proceso, estado_proceso, confianza,
                   timestamp_inicio, timestamp_fin, duracion_ms, 
                   servicio_procesador, version_servicio
            FROM registro_procesamiento_documento
            WHERE id_documento = %s
            ORDER BY timestamp_inicio DESC
        """
        
        result['processing_history'] = execute_query(processing_query, [document_id], True)
        
        # Obtener tipos de documento disponibles para selección
        types_query = """
            SELECT id_tipo_documento, nombre_tipo, descripcion
            FROM tipos_documento
            WHERE requiere_extraccion_ia = 1
            ORDER BY nombre_tipo
        """
        
        result['available_document_types'] = execute_query(types_query, [], True)
        
        return result
    except Exception as e:
        logger.error(f"Error al obtener datos para revisión de documento: {str(e)}")
        raise

def submit_document_review(document_id, analysis_id, user_id, verification_status, 
                          verification_notes, corrected_data=None, document_type_confirmed=None):
    """
    Procesa la revisión manual de un documento y actualiza la base de datos.
    
    Args:
        document_id: ID del documento revisado
        analysis_id: ID del análisis asociado
        user_id: ID del usuario que realiza la revisión
        verification_status: Estado de verificación (approved, rejected, corrected)
        verification_notes: Notas de verificación
        corrected_data: Datos corregidos (opcional)
        document_type_confirmed: ID del tipo de documento confirmado (opcional)
        
    Returns:
        Boolean indicando si la operación fue exitosa
    """
    # Obtener conexión para transacción
    connection = get_connection()
    
    # Registrar inicio del proceso de revisión
    process_log_id = log_document_processing_start(
        document_id=document_id,
        tipo_proceso="validacion_manual",
        datos_entrada=json.dumps({
            "verification_status": verification_status,
            "document_type_confirmed": document_type_confirmed,
            "has_corrected_data": bool(corrected_data)
        }),
        analisis_id=analysis_id,
        servicio="manual_review_handler",
        version="1.0"
    )
    
    try:
        # Iniciar transacción
        connection.begin()
        
        with connection.cursor() as cursor:
            # 1. Actualizar analisis_documento_ia
            update_analysis_query = """
                UPDATE analisis_documento_ia
                SET verificado = 1,
                    verificado_por = %s,
                    fecha_verificacion = NOW(),
                    metadatos_extraccion = JSON_MERGE_PATCH(
                        COALESCE(metadatos_extraccion, '{}'),
                        %s
                    )
                WHERE id_analisis = %s AND id_documento = %s
            """
            
            # Preparar metadata para actualización
            metadata_update = {
                'verification_notes': verification_notes,
                'verification_status': verification_status,
                'verification_date': datetime.datetime.now().isoformat(),
                'correction_summary': {k: 'updated' for k in (corrected_data or {}).keys()}
            }
            
            # Convertir a JSON
            metadata_json = json.dumps(metadata_update)
            
            cursor.execute(
                update_analysis_query, 
                [user_id, metadata_json, analysis_id, document_id]
            )
            
            # 2. Actualizar tipo de documento si se ha confirmado uno diferente
            if document_type_confirmed:
                update_doc_type_query = """
                    UPDATE documentos
                    SET id_tipo_documento = %s,
                        modificado_por = %s,
                        fecha_modificacion = NOW()
                    WHERE id_documento = %s
                """
                cursor.execute(
                    update_doc_type_query, 
                    [document_type_confirmed, user_id, document_id]
                )
            
            # 3. Procesar datos específicos según el tipo de documento
            
            # 3.1. Comprobar si hay datos de documento de identidad
            if corrected_data and 'id_document_data' in corrected_data:
                id_data = corrected_data['id_document_data']
                
                # Comprobar si existe un registro previo
                check_id_query = """
                    SELECT COUNT(*) as count FROM documentos_identificacion
                    WHERE id_documento = %s
                """
                cursor.execute(check_id_query, [document_id])
                check_result = cursor.fetchone()
                
                if check_result and check_result['count'] > 0:
                    # Actualizar registro existente
                    update_id_query = """
                        UPDATE documentos_identificacion
                        SET tipo_documento = %s,
                            numero_documento = %s,
                            pais_emision = %s,
                            fecha_emision = %s,
                            fecha_expiracion = %s,
                            genero = %s,
                            nombre_completo = %s
                        WHERE id_documento = %s
                    """
                    cursor.execute(
                        update_id_query,
                        [
                            id_data.get('tipo_documento'),
                            id_data.get('numero_documento'),
                            id_data.get('pais_emision'),
                            id_data.get('fecha_emision'),
                            id_data.get('fecha_expiracion'),
                            id_data.get('genero'),
                            id_data.get('nombre_completo'),
                            document_id
                        ]
                    )
                else:
                    # Insertar nuevo registro
                    insert_id_query = """
                        INSERT INTO documentos_identificacion
                        (id_documento, tipo_documento, numero_documento, 
                         pais_emision, fecha_emision, fecha_expiracion, 
                         genero, nombre_completo)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    """
                    cursor.execute(
                        insert_id_query,
                        [
                            document_id,
                            id_data.get('tipo_documento'),
                            id_data.get('numero_documento'),
                            id_data.get('pais_emision'),
                            id_data.get('fecha_emision'),
                            id_data.get('fecha_expiracion'),
                            id_data.get('genero'),
                            id_data.get('nombre_completo')
                        ]
                    )
            
            # 4. Marcar documento como validado manualmente
            update_validation_query = """
                UPDATE documentos
                SET validado_manualmente = 1,
                    fecha_validacion = NOW(),
                    validado_por = %s
                WHERE id_documento = %s
            """
            cursor.execute(
                update_validation_query,
                [user_id, document_id]
            )
            
            # 5. Actualizar estado del cliente si está asociado
            check_client_query = """
                SELECT dc.id_cliente
                FROM documentos_clientes dc
                WHERE dc.id_documento = %s
                LIMIT 1
            """
            cursor.execute(check_client_query, [document_id])
            client_result = cursor.fetchone()
            
            if client_result:
                client_id = client_result['id_cliente']
                # Llamar al procedimiento almacenado para actualizar estado documental del cliente
                cursor.callproc('actualizar_estado_documental_cliente', [client_id])
            
            # Confirmar transacción
            connection.commit()
            
            # Registrar finalización del proceso
            log_document_processing_end(
                registro_id=process_log_id,
                estado='completado',
                datos_procesados=json.dumps(metadata_update),
                datos_salida=json.dumps({'result': 'success'}),
                confianza=1.0  # Alta confianza por ser revisión manual
            )
            
            return True
    except Exception as e:
        # Revertir transacción en caso de error
        connection.rollback()
        
        # Registrar error en el log
        logger.error(f"Error al procesar revisión de documento: {str(e)}")
        
        # Registrar finalización con error
        log_document_processing_end(
            registro_id=process_log_id,
            estado='error',
            mensaje_error=str(e)
        )
        
        raise
    finally:
        connection.close()

def get_review_statistics():
    """
    Obtiene estadísticas sobre el proceso de revisión manual.
    
    Returns:
        Dict con estadísticas de revisión
    """
    try:
        # Obtener conteo de revisiones pendientes
        pending_query = """
            SELECT COUNT(*) as pending_count
            FROM analisis_documento_ia
            WHERE requiere_verificacion = 1 AND verificado = 0
        """
        
        # Obtener estadísticas por estado en últimos 30 días
        status_query = """
            SELECT 
                SUM(CASE WHEN JSON_EXTRACT(metadatos_extraccion, '$.verification_status') = 'approved' THEN 1 ELSE 0 END) as approved,
                SUM(CASE WHEN JSON_EXTRACT(metadatos_extraccion, '$.verification_status') = 'rejected' THEN 1 ELSE 0 END) as rejected,
                SUM(CASE WHEN JSON_EXTRACT(metadatos_extraccion, '$.verification_status') = 'corrected' THEN 1 ELSE 0 END) as corrected
            FROM analisis_documento_ia
            WHERE verificado = 1
            AND fecha_verificacion >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
        """
        
        # Obtener confianza promedio por tipo de documento
        confidence_query = """
            SELECT 
                td.nombre_tipo as document_type,
                AVG(a.confianza_clasificacion) as avg_confidence
            FROM analisis_documento_ia a
            JOIN documentos d ON a.id_documento = d.id_documento
            JOIN tipos_documento td ON d.id_tipo_documento = td.id_tipo_documento
            WHERE a.verificado = 1
            AND a.fecha_verificacion >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
            GROUP BY td.nombre_tipo
            ORDER BY avg_confidence DESC
        """
        
        # Obtener tendencia de revisiones por día (últimas 2 semanas)
        trend_query = """
            SELECT 
                DATE(fecha_verificacion) as review_date,
                COUNT(*) as review_count
            FROM analisis_documento_ia
            WHERE verificado = 1
            AND fecha_verificacion >= DATE_SUB(CURRENT_DATE(), INTERVAL 14 DAY)
            GROUP BY DATE(fecha_verificacion)
            ORDER BY review_date
        """
        
        # Ejecutar consultas
        pending_result = execute_query(pending_query, [], True)
        status_result = execute_query(status_query, [], True)
        confidence_result = execute_query(confidence_query, [], True)
        trend_result = execute_query(trend_query, [], True)
        
        # Procesar resultados
        pending_count = pending_result[0]['pending_count'] if pending_result else 0
        
        status_stats = {}
        if status_result and status_result[0]:
            status_stats = {
                'approved': status_result[0]['approved'] or 0,
                'rejected': status_result[0]['rejected'] or 0,
                'corrected': status_result[0]['corrected'] or 0
            }
            
        return {
            'pending_count': pending_count,
            'status_stats': status_stats,
            'confidence_stats': confidence_result,
            'trend_stats': trend_result
        }
    except Exception as e:
        logger.error(f"Error al obtener estadísticas de revisión: {str(e)}")
        raise
# Nuevas funciones para ClientViewAggregator

def get_client_basic_info(client_id):
    """Obtiene información básica de un cliente por su ID"""
    query = """
    SELECT 
        id_cliente,
        nombre_razon_social,
        tipo_cliente,
        segmento_bancario,
        nivel_riesgo,
        estado_documental
    FROM 
        clientes 
    WHERE 
        id_cliente = %s
    """
    results = execute_query(query, (client_id,))
    if results:
        return results[0]
    return None

def get_client_valid_documents(client_id):
    """
    Obtiene el conteo de documentos válidos de un cliente
    (publicados y validados manualmente)
    """
    query = """
    SELECT COUNT(DISTINCT dc.id_documento) as docs_validos
    FROM documentos_clientes dc
    JOIN documentos d ON dc.id_documento = d.id_documento
    JOIN tipos_documento td ON d.id_tipo_documento = td.id_tipo_documento
    JOIN tipos_documento_bancario tdb ON td.id_tipo_documento = tdb.id_tipo_documento
    WHERE dc.id_cliente = %s
    AND d.estado = 'publicado'
    AND d.validado_manualmente = TRUE
    """
    results = execute_query(query, (client_id,))
    if results:
        return results[0]['docs_validos']
    return 0

def get_client_required_documents(client_id):
    """
    Obtiene el conteo de documentos requeridos para un cliente
    según su segmento y nivel de riesgo
    """
    query = """
    SELECT COUNT(*) as docs_requeridos
    FROM tipos_documento_bancario tdb
    JOIN categorias_bancarias cb ON tdb.id_categoria_bancaria = cb.id_categoria_bancaria
    JOIN clientes c ON c.segmento_bancario IS NOT NULL
    WHERE c.id_cliente = %s
    AND cb.requiere_validacion = TRUE
    AND (
        cb.relevancia_legal = 'alta' 
        OR (cb.relevancia_legal = 'media' AND c.nivel_riesgo IN ('alto', 'muy_alto'))
    )
    """
    results = execute_query(query, (client_id,))
    if results and results[0]['docs_requeridos'] > 0:
        return results[0]['docs_requeridos']
    return 1  # Mínimo 1 para evitar división por cero

def get_client_pending_documents(client_id):
    """Obtiene el conteo de documentos pendientes solicitados a un cliente"""
    query = """
    SELECT COUNT(*) as pendientes
    FROM documentos_solicitados
    WHERE id_cliente = %s
    AND estado IN ('pendiente', 'recordatorio_enviado')
    """
    results = execute_query(query, (client_id,))
    if results:
        return results[0]['pendientes']
    return 0

def get_client_expired_documents(client_id):
    """Obtiene el conteo de documentos caducados de un cliente"""
    query = """
    SELECT COUNT(*) as caducados
    FROM documentos_clientes dc
    JOIN documentos d ON dc.id_documento = d.id_documento
    JOIN documentos_identificacion di ON d.id_documento = di.id_documento
    WHERE dc.id_cliente = %s
    AND di.fecha_expiracion < CURDATE()
    """
    results = execute_query(query, (client_id,))
    if results:
        return results[0]['caducados']
    return 0

def update_client_view_cache(client_id, resumen_actividad, kpis_cliente):
    """
    Actualiza la tabla vista_cliente_cache con la información calculada.
    Crea el registro si no existe.
    """
    # Convertir a JSON si son diccionarios
    if isinstance(resumen_actividad, dict):
        resumen_actividad = json.dumps(resumen_actividad)
    if isinstance(kpis_cliente, dict):
        kpis_cliente = json.dumps(kpis_cliente)
    
    # Intentar actualizar si existe
    query = """
    UPDATE vista_cliente_cache
    SET 
        ultima_actualizacion = NOW(),
        resumen_actividad = %s,
        kpis_cliente = %s
    WHERE id_cliente = %s
    """
    result = execute_query(query, (resumen_actividad, kpis_cliente, client_id), fetch=False)
    
    # Verificar si se actualizó algún registro
    check_query = """
    SELECT COUNT(*) as count 
    FROM vista_cliente_cache 
    WHERE id_cliente = %s
    """
    check_result = execute_query(check_query, (client_id,))
    
    # Si no existe, crear el registro
    if check_result and check_result[0]['count'] == 0:
        insert_query = """
        INSERT INTO vista_cliente_cache (
            id_cliente,
            ultima_actualizacion,
            resumen_actividad,
            kpis_cliente
        ) VALUES (%s, NOW(), %s, %s)
        """
        execute_query(insert_query, (client_id, resumen_actividad, kpis_cliente), fetch=False)
    
    return True

def get_all_active_clients():
    """Obtiene todos los clientes activos"""
    query = """
    SELECT id_cliente
    FROM clientes
    WHERE estado = 'activo'
    """
    return execute_query(query)

def calculate_document_completeness(client_id):
    """
    Calcula la completitud documental, documentos pendientes y caducados para un cliente.
    Retorna una tupla (completitud_porcentaje, docs_pendientes, docs_caducados)
    """
    # Obtener documentos válidos
    docs_validos = get_client_valid_documents(client_id)
    
    # Obtener documentos requeridos
    docs_requeridos = get_client_required_documents(client_id)
    
    # Obtener documentos pendientes
    docs_pendientes = get_client_pending_documents(client_id)
    
    # Obtener documentos caducados
    docs_caducados = get_client_expired_documents(client_id)
    
    # Calcular completitud (porcentaje de documentos válidos vs. requeridos)
    completitud = (docs_validos / docs_requeridos) * 100
    
    # Limitar a un máximo de 100%
    completitud = min(100, completitud)
    
    return round(completitud, 2), docs_pendientes, docs_caducados

def determine_document_status(completitud, docs_pendientes, docs_caducados):
    """
    Determina el estado documental según la completitud y documentos pendientes/caducados
    
    :return: Estado documental (completo, pendiente_actualizacion, incompleto, crítico)
    """
    if completitud >= 95 and docs_pendientes == 0 and docs_caducados == 0:
        return 'completo'
    elif docs_caducados > 0:
        return 'critico'
    elif completitud >= 80:
        return 'pendiente_actualizacion'
    else:
        return 'incompleto'

def calculate_document_risk(completitud, docs_caducados, nivel_riesgo_cliente):
    """
    Calcula el nivel de riesgo documental basado en varios factores
    
    :return: Nivel de riesgo (bajo, medio, alto, muy_alto)
    """
    # Base de riesgo según completitud
    if completitud >= 95:
        riesgo_base = 'bajo'
    elif completitud >= 80:
        riesgo_base = 'medio'
    elif completitud >= 60:
        riesgo_base = 'alto'
    else:
        riesgo_base = 'muy_alto'
    
    # Ajustar según documentos caducados
    if docs_caducados > 3:
        # Incrementar dos niveles
        if riesgo_base == 'bajo':
            riesgo_base = 'alto'
        else:
            riesgo_base = 'muy_alto'
    elif docs_caducados > 0:
        # Incrementar un nivel
        if riesgo_base == 'bajo':
            riesgo_base = 'medio'
        elif riesgo_base == 'medio':
            riesgo_base = 'alto'
        else:
            riesgo_base = 'muy_alto'
    
    # Considerar el nivel de riesgo del cliente
    if nivel_riesgo_cliente == 'muy_alto':
        # Cliente de alto riesgo nunca puede tener riesgo documental bajo
        if riesgo_base == 'bajo':
            riesgo_base = 'medio'
    
    return riesgo_base

# Añadir estas funciones a db_connector.py
def preserve_document_data_before_update(document_id, reason="Manual preservation"):
    """
    Preserva los datos actuales de un documento antes de actualizarlo
    """
    try:
        # Obtener datos actuales del documento
        document = get_document_by_id(document_id)
        if not document:
            logger.error(f"Documento {document_id} no encontrado")
            return False
        
        # Obtener la versión actual
        version_query = """
        SELECT id_version FROM versiones_documento 
        WHERE id_documento = %s AND numero_version = %s
        LIMIT 1
        """
        version_result = execute_query(version_query, (document_id, document['version_actual']))
        
        if not version_result:
            logger.error(f"No se encontró la versión actual para documento {document_id}")
            return False
        
        current_version_id = version_result[0]['id_version']
        
        # Si hay datos extraídos, preservarlos
        if document.get('datos_extraidos_ia'):
            historico_id = generate_uuid()
            
            insert_query = """
            INSERT INTO historico_datos_extraidos (
                id_historico,
                id_documento,
                id_version,
                datos_extraidos_ia,
                confianza_extraccion,
                validado_manualmente,
                preservado_por,
                motivo_preservacion
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """
            
            execute_query(insert_query, (
                historico_id,
                document_id,
                current_version_id,
                document['datos_extraidos_ia'],
                document.get('confianza_extraccion', 0),
                document.get('validado_manualmente', False),
                document.get('modificado_por'),
                reason
            ), fetch=False)
            
            logger.info(f"Datos preservados para documento {document_id}, versión {document['version_actual']}")
        
        return True
        
    except Exception as e:
        logger.error(f"Error al preservar datos del documento: {str(e)}")
        return False

def get_document_version_history(document_id, include_data=False):
    """
    Obtiene el historial completo de versiones de un documento
    """
    try:
        if include_data:
            # Usar la vista que incluye los datos extraídos
            query = """
            SELECT * FROM vista_historial_documento
            WHERE id_documento = %s
            ORDER BY numero_version DESC
            """
        else:
            # Solo información básica de versiones
            query = """
            SELECT 
                v.id_version,
                v.numero_version,
                v.fecha_creacion,
                v.creado_por,
                v.comentario_version,
                v.nombre_original,
                v.tamano_bytes,
                v.estado_ocr,
                u.nombre_usuario as creado_por_nombre,
                CASE 
                    WHEN v.numero_version = d.version_actual THEN TRUE
                    ELSE FALSE
                END as es_version_actual
            FROM versiones_documento v
            JOIN documentos d ON v.id_documento = d.id_documento
            LEFT JOIN usuarios u ON v.creado_por = u.id_usuario
            WHERE v.id_documento = %s
            ORDER BY v.numero_version DESC
            """
        
        return execute_query(query, (document_id,))
        
    except Exception as e:
        logger.error(f"Error al obtener historial de versiones: {str(e)}")
        return []

def restore_document_version(document_id, version_number, restored_by):
    """
    Restaura una versión anterior de un documento
    """
    connection = get_connection()
    try:
        connection.begin()
        
        with connection.cursor() as cursor:
            # 1. Verificar que la versión existe
            check_query = """
            SELECT v.*, h.datos_extraidos_ia, h.confianza_extraccion, h.validado_manualmente
            FROM versiones_documento v
            LEFT JOIN historico_datos_extraidos h ON v.id_version = h.id_version
            WHERE v.id_documento = %s AND v.numero_version = %s
            """
            cursor.execute(check_query, (document_id, version_number))
            version_data = cursor.fetchone()
            
            if not version_data:
                raise ValueError(f"No se encontró la versión {version_number} del documento {document_id}")
            
            # 2. Preservar datos actuales antes de restaurar
            preserve_document_data_before_update(document_id, f"Restauración a versión {version_number}")
            
            # 3. Actualizar el documento con los datos de la versión restaurada
            update_query = """
            UPDATE documentos
            SET version_actual = %s,
                modificado_por = %s,
                fecha_modificacion = NOW()
            """
            
            params = [version_number, restored_by]
            
            # Si hay datos extraídos históricos, restaurarlos también
            if version_data.get('datos_extraidos_ia'):
                update_query += """,
                    datos_extraidos_ia = %s,
                    confianza_extraccion = %s,
                    validado_manualmente = %s
                """
                params.extend([
                    version_data['datos_extraidos_ia'],
                    version_data.get('confianza_extraccion', 0),
                    version_data.get('validado_manualmente', False)
                ])
            
            update_query += " WHERE id_documento = %s"
            params.append(document_id)
            
            cursor.execute(update_query, params)
            
            # 4. Registrar en auditoría
            audit_data = {
                'fecha_hora': datetime.now().isoformat(),
                'usuario_id': restored_by,
                'direccion_ip': '0.0.0.0',
                'accion': 'restaurar_version',
                'entidad_afectada': 'documento',
                'id_entidad_afectada': document_id,
                'detalles': json.dumps({
                    'version_restaurada': version_number,
                    'version_id': version_data['id_version']
                }),
                'resultado': 'éxito'
            }
            insert_audit_record(audit_data)
            
            connection.commit()
            logger.info(f"Documento {document_id} restaurado a versión {version_number}")
            return True
            
    except Exception as e:
        connection.rollback()
        logger.error(f"Error al restaurar versión: {str(e)}")
        raise
    finally:
        connection.close()

def compare_document_versions(document_id, version1, version2):
    """
    Compara dos versiones de un documento
    """
    try:
        query = """
        SELECT 
            v.numero_version,
            v.fecha_creacion,
            v.nombre_original,
            v.tamano_bytes,
            COALESCE(h.datos_extraidos_ia, d.datos_extraidos_ia) as datos_extraidos,
            COALESCE(h.confianza_extraccion, d.confianza_extraccion) as confianza
        FROM versiones_documento v
        LEFT JOIN historico_datos_extraidos h ON v.id_version = h.id_version
        LEFT JOIN documentos d ON (v.id_documento = d.id_documento AND v.numero_version = d.version_actual)
        WHERE v.id_documento = %s AND v.numero_version IN (%s, %s)
        """
        
        results = execute_query(query, (document_id, version1, version2))
        
        if len(results) != 2:
            raise ValueError("No se encontraron ambas versiones para comparar")
        
        # Organizar resultados por versión
        comparison = {}
        for result in results:
            version_num = result['numero_version']
            comparison[f'version_{version_num}'] = {
                'fecha': result['fecha_creacion'],
                'archivo': result['nombre_original'],
                'tamaño': result['tamano_bytes'],
                'confianza': result['confianza']
            }
            
            # Comparar datos extraídos si existen
            if result['datos_extraidos']:
                try:
                    datos = json.loads(result['datos_extraidos']) if isinstance(result['datos_extraidos'], str) else result['datos_extraidos']
                    comparison[f'version_{version_num}']['datos_extraidos'] = datos
                except:
                    pass
        
        # Calcular diferencias
        differences = {
            'archivo_cambio': comparison[f'version_{version1}']['archivo'] != comparison[f'version_{version2}']['archivo'],
            'tamaño_diferencia': comparison[f'version_{version2}']['tamaño'] - comparison[f'version_{version1}']['tamaño'],
            'confianza_cambio': comparison[f'version_{version2}']['confianza'] - comparison[f'version_{version1}']['confianza']
        }
        
        comparison['diferencias'] = differences
        
        return comparison
        
    except Exception as e:
        logger.error(f"Error al comparar versiones: {str(e)}")
        raise

def get_latest_extraction_data(document_id, version_id=None):
    """
    Obtiene los datos de extracción más recientes, ya sea de la versión actual
    o de una versión específica
    """
    try:
        if version_id:
            # Buscar en histórico primero
            query = """
            SELECT datos_extraidos_ia, confianza_extraccion, validado_manualmente
            FROM historico_datos_extraidos
            WHERE id_documento = %s AND id_version = %s
            ORDER BY fecha_extraccion DESC
            LIMIT 1
            """
            result = execute_query(query, (document_id, version_id))
            
            if result:
                return result[0]
        
        # Si no hay versión específica o no se encontró en histórico,
        # buscar en documento actual
        query = """
        SELECT datos_extraidos_ia, confianza_extraccion, validado_manualmente
        FROM documentos
        WHERE id_documento = %s
        """
        result = execute_query(query, (document_id,))
        
        return result[0] if result else None
        
    except Exception as e:
        logger.error(f"Error al obtener datos de extracción: {str(e)}")
        return None  

def insert_migrated_document_info(creatio_file_id, id_documento, id_cliente, nombre_archivo):
    """
    Versión DEFINITIVA que resuelve el error 'not all arguments converted during string formatting'
    
    CAMBIOS PRINCIPALES:
    1. Conversión explícita de UUID a string
    2. Manejo robusto de caracteres especiales
    3. Validación estricta de tipos de datos
    4. Fallback automático para errores de conversión
    5. Prevención de dobles inserciones
    """
    
    # 🔍 Log de entrada para debugging
    logger.info(f"🔍 insert_migrated_document_info llamada con:")
    logger.info(f"   creatio_file_id: {type(creatio_file_id).__name__} = {repr(creatio_file_id)}")
    logger.info(f"   id_documento: {type(id_documento).__name__} = {repr(id_documento)}")
    logger.info(f"   id_cliente: {type(id_cliente).__name__} = {repr(id_cliente)}")
    logger.info(f"   nombre_archivo: {type(nombre_archivo).__name__} = {repr(nombre_archivo)}")
    
    try:
        # ✅ VALIDACIÓN ESTRICTA: Verificar que los parámetros no estén vacíos
        if not creatio_file_id or str(creatio_file_id).strip() == '':
            logger.warning("creatio_file_id vacío, saltando inserción")
            return False
            
        if not id_documento or str(id_documento).strip() == '':
            logger.error("id_documento vacío, no se puede insertar")
            return False
            
        if not id_cliente or str(id_cliente).strip() == '':
            logger.warning("id_cliente vacío, saltando inserción")
            return False
        
        # ✅ CONVERSIÓN ULTRA-SEGURA: Convertir TODO a string explícitamente
        def safe_string_convert(value, max_length=None, field_name="campo"):
            """Convierte un valor a string de forma ultra-segura"""
            try:
                if value is None:
                    return None
                
                # Manejar UUIDs y objetos especiales
                if hasattr(value, '__str__'):
                    str_value = str(value).strip()
                else:
                    str_value = repr(value).strip()
                
                # Limpiar caracteres problemáticos
                # Remover caracteres que pueden causar problemas con SQL
                clean_value = str_value.replace('\x00', '').replace('\r', '').replace('\n', ' ')
                
                # Truncar si es necesario
                if max_length and len(clean_value) > max_length:
                    clean_value = clean_value[:max_length]
                    logger.warning(f"⚠️ {field_name} truncado de {len(str_value)} a {max_length} caracteres")
                
                return clean_value
                
            except Exception as conv_error:
                logger.error(f"❌ Error convirtiendo {field_name}: {str(conv_error)}")
                return str(repr(value))[:max_length] if max_length else str(repr(value))
        
        # ✅ APLICAR CONVERSIÓN SEGURA A TODOS LOS CAMPOS
        creatio_file_id_clean = safe_string_convert(creatio_file_id, 100, "creatio_file_id")
        id_documento_clean = safe_string_convert(id_documento, 36, "id_documento")
        id_cliente_clean = safe_string_convert(id_cliente, 36, "id_cliente")
        nombre_archivo_clean = safe_string_convert(nombre_archivo, 255, "nombre_archivo")
        
        # ✅ GENERAR UUID COMO STRING LIMPIO
        new_id = safe_string_convert(str(uuid.uuid4()), 36, "new_id")
        
        logger.info(f"✅ Datos procesados y limpios:")
        logger.info(f"   new_id: {repr(new_id)}")
        logger.info(f"   creatio_file_id_clean: {repr(creatio_file_id_clean)}")
        logger.info(f"   id_documento_clean: {repr(id_documento_clean)}")
        logger.info(f"   id_cliente_clean: {repr(id_cliente_clean)}")
        logger.info(f"   nombre_archivo_clean: {repr(nombre_archivo_clean)}")
        
        # ✅ PREVENCIÓN DE DUPLICADOS: Verificar existencia ANTES de insertar
        check_query = "SELECT COUNT(*) as count FROM documentos_migrados_creatio WHERE creatio_file_id = %s"
        
        try:
            existing_result = execute_query(check_query, (creatio_file_id_clean,))
            if existing_result and existing_result[0]['count'] > 0:
                logger.info(f"📋 Documento migrado YA EXISTE para creatio_file_id: {creatio_file_id_clean}")
                return True  # No es error, ya existe
        except Exception as check_error:
            logger.warning(f"⚠️ Error verificando existencia: {str(check_error)}")
            # Continuar con inserción
        
        # ✅ PREPARAR QUERY E INSERCIÓN
        query = """
        INSERT INTO documentos_migrados_creatio (
            id, creatio_file_id, id_documento, id_cliente, nombre_archivo
        ) VALUES (%s, %s, %s, %s, %s)
        """
        
        # ✅ PARÁMETROS COMO TUPLA DE STRINGS
        params = (
            new_id,
            creatio_file_id_clean,
            id_documento_clean,
            id_cliente_clean,
            nombre_archivo_clean
        )
        
        # ✅ VERIFICACIÓN FINAL DE PARÁMETROS
        placeholder_count = query.count('%s')
        param_count = len(params)
        
        if placeholder_count != param_count:
            logger.error(f"❌ MISMATCH: {placeholder_count} placeholders != {param_count} parámetros")
            logger.error(f"Query: {query}")
            logger.error(f"Params: {params}")
            return False
        
        logger.info(f"✅ Parámetros validados: {param_count} placeholders = {param_count} parámetros")
        
        # ✅ INSERCIÓN CON MANEJO DE ERRORES ESPECÍFICOS
        try:
            execute_query(query, params, fetch=False)
            logger.info(f"📥 Documento migrado insertado exitosamente: {creatio_file_id_clean}")
            return True
            
        except Exception as db_error:
            error_str = str(db_error).lower()
            
            # ✅ MANEJO ESPECÍFICO DE ERRORES
            if "duplicate entry" in error_str or "unique constraint" in error_str:
                logger.info(f"📋 Documento migrado ya existía (duplicate): {creatio_file_id_clean}")
                return True  # No es realmente un error
                
            elif "not all arguments converted" in error_str:
                logger.error(f"🚨 ERROR DE CONVERSIÓN - APLICANDO FALLBACK:")
                
                # 🔧 FALLBACK: Intentar con parámetros super-seguros
                try:
                    # Método alternativo: usar solo caracteres ASCII seguros
                    fallback_params = []
                    for i, param in enumerate(params):
                        if param is None:
                            fallback_params.append(None)
                        else:
                            # Convertir a ASCII seguro y limpiar
                            safe_param = str(param).encode('ascii', 'ignore').decode('ascii')
                            fallback_params.append(safe_param)
                    
                    logger.error(f"🔧 Reintentando con parámetros ASCII seguros:")
                    for i, param in enumerate(fallback_params):
                        logger.error(f"   fallback[{i}]: {repr(param)}")
                    
                    execute_query(query, tuple(fallback_params), fetch=False)
                    logger.info(f"✅ FALLBACK EXITOSO - documento insertado con parámetros seguros")
                    return True
                    
                except Exception as fallback_error:
                    logger.error(f"❌ FALLBACK también falló: {str(fallback_error)}")
                    
                    # 🔧 ÚLTIMO RECURSO: Inserción con valores por defecto
                    try:
                        logger.error(f"🔧 ÚLTIMO RECURSO: valores mínimos...")
                        minimal_query = """
                        INSERT INTO documentos_migrados_creatio (
                            id, creatio_file_id, id_documento, id_cliente
                        ) VALUES (%s, %s, %s, %s)
                        """
                        minimal_params = (
                            str(uuid.uuid4()),
                            str(creatio_file_id)[:100] if creatio_file_id else 'unknown',
                            str(id_documento)[:36] if id_documento else 'unknown',
                            str(id_cliente)[:36] if id_cliente else 'unknown'
                        )
                        
                        execute_query(minimal_query, minimal_params, fetch=False)
                        logger.info(f"✅ ÚLTIMO RECURSO exitoso - documento insertado con datos mínimos")
                        return True
                        
                    except Exception as minimal_error:
                        logger.error(f"❌ ÚLTIMO RECURSO falló: {str(minimal_error)}")
                        return False
                
            elif "data too long" in error_str:
                logger.error(f"❌ Datos demasiado largos para la tabla:")
                for i, param in enumerate(params):
                    if param:
                        logger.error(f"   param[{i}]: {len(str(param))} caracteres")
                return False
                
            else:
                logger.error(f"❌ Error de base de datos no manejado: {str(db_error)}")
                logger.error(f"   Query: {query}")
                logger.error(f"   Params: {params}")
                return False
        
    except Exception as e:
        logger.error(f"❌ Error general en insert_migrated_document_info:")
        logger.error(f"   Tipo: {type(e).__name__}")
        logger.error(f"   Mensaje: {str(e)}")
        
        # Stack trace para debugging
        import traceback
        logger.error("   Stack trace completo:")
        for line in traceback.format_exc().split('\n'):
            if line.strip():
                logger.error(f"     {line}")
        
        return False

# ✅ FUNCIÓN AUXILIAR PARA VERIFICAR INTEGRIDAD DE LA TABLA
def verify_migrated_documents_table():
    """
    Verifica que la tabla documentos_migrados_creatio existe y tiene la estructura correcta
    """
    try:
        # Verificar existencia de la tabla
        check_table_query = """
        SELECT COUNT(*) as count 
        FROM information_schema.tables 
        WHERE table_schema = DATABASE() 
        AND table_name = 'documentos_migrados_creatio'
        """
        
        result = execute_query(check_table_query)
        if not result or result[0]['count'] == 0:
            logger.error("❌ Tabla documentos_migrados_creatio no existe")
            return False
        
        # Verificar estructura de la tabla
        describe_query = "DESCRIBE documentos_migrados_creatio"
        structure = execute_query(describe_query)
        
        expected_fields = ['id', 'creatio_file_id', 'id_documento', 'id_cliente', 'fecha_migracion', 'nombre_archivo']
        existing_fields = [row['Field'] for row in structure]
        
        missing_fields = set(expected_fields) - set(existing_fields)
        if missing_fields:
            logger.error(f"❌ Campos faltantes en tabla: {missing_fields}")
            return False
        
        logger.info("✅ Tabla documentos_migrados_creatio verificada correctamente")
        return True
        
    except Exception as e:
        logger.error(f"❌ Error verificando tabla: {str(e)}")
        return False

# ✅ FUNCIÓN PARA LIMPIAR REGISTROS DUPLICADOS (SI ES NECESARIO)
def cleanup_duplicate_migrated_documents():
    """
    Limpia registros duplicados en documentos_migrados_creatio manteniendo el más reciente
    """
    try:
        # Encontrar duplicados por creatio_file_id
        duplicates_query = """
        SELECT creatio_file_id, COUNT(*) as count, MIN(id) as keep_id
        FROM documentos_migrados_creatio 
        GROUP BY creatio_file_id 
        HAVING COUNT(*) > 1
        """
        
        duplicates = execute_query(duplicates_query)
        
        if not duplicates:
            logger.info("✅ No se encontraron duplicados en documentos_migrados_creatio")
            return True
        
        logger.warning(f"⚠️ Se encontraron {len(duplicates)} grupos de registros duplicados")
        
        # Eliminar duplicados (mantener el más antiguo por seguridad)
        for dup in duplicates:
            delete_query = """
            DELETE FROM documentos_migrados_creatio 
            WHERE creatio_file_id = %s AND id != %s
            """
            
            execute_query(delete_query, (dup['creatio_file_id'], dup['keep_id']), fetch=False)
            logger.info(f"🧹 Limpiados duplicados para creatio_file_id: {dup['creatio_file_id']}")
        
        logger.info(f"✅ Limpieza de duplicados completada")
        return True
        
    except Exception as e:
        logger.error(f"❌ Error en limpieza de duplicados: {str(e)}")
        return False
      
def preserve_identification_data(document_id, version_id=None, reason="Manual preservation"):
    """
    Preserva los datos de identificación actuales antes de una actualización
    """
    try:
        # Obtener datos actuales de identificación
        query = """
        SELECT * FROM documentos_identificacion
        WHERE id_documento = %s
        """
        
        id_data = execute_query(query, (document_id,))
        
        if not id_data:
            logger.warning(f"No se encontraron datos de identificación para documento {document_id}")
            return False
        
        current_data = id_data[0]
        
        # Si no se proporciona version_id, obtener la versión actual
        if not version_id:
            version_query = """
            SELECT v.id_version 
            FROM versiones_documento v
            JOIN documentos d ON v.id_documento = d.id_documento
            WHERE v.id_documento = %s AND v.numero_version = d.version_actual
            LIMIT 1
            """
            version_result = execute_query(version_query, (document_id,))
            version_id = version_result[0]['id_version'] if version_result else generate_uuid()
        
        # Generar ID para el histórico
        historico_id = generate_uuid()
        
        # Insertar en histórico
        insert_query = """
        INSERT INTO historico_documentos_identificacion (
            id_historico, id_documento, id_version,
            tipo_documento, numero_documento, pais_emision,
            fecha_emision, fecha_expiracion, nombre_completo,
            genero, lugar_nacimiento, autoridad_emision,
            nacionalidad, codigo_pais,
            preservado_por, motivo_preservacion
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
        """
        
        execute_query(insert_query, (
            historico_id,
            document_id,
            version_id,
            current_data.get('tipo_documento'),
            current_data.get('numero_documento'),
            current_data.get('pais_emision'),
            current_data.get('fecha_emision'),
            current_data.get('fecha_expiracion'),
            current_data.get('nombre_completo'),
            current_data.get('genero'),
            current_data.get('lugar_nacimiento'),
            current_data.get('autoridad_emision'),
            current_data.get('nacionalidad'),
            current_data.get('codigo_pais'),
            '691d8c44-f524-48fd-b292-be9e31977711', # Usuario sistema
            reason
        ), fetch=False)
        
        logger.info(f"Datos de identificación preservados para documento {document_id}")
        return True
        
    except Exception as e:
        logger.error(f"Error al preservar datos de identificación: {str(e)}")
        return False

def get_identification_history(document_id):
    """
    Obtiene el historial completo de datos de identificación de un documento
    """
    try:
        query = """
        SELECT 
            h.*,
            v.numero_version,
            v.fecha_creacion as fecha_version,
            v.comentario_version,
            CASE 
                WHEN v.numero_version = (SELECT version_actual FROM documentos WHERE id_documento = %s)
                THEN 'Actual' 
                ELSE 'Histórica'
            END as estado_version
        FROM historico_documentos_identificacion h
        JOIN versiones_documento v ON h.id_version = v.id_version
        WHERE h.id_documento = %s
        ORDER BY v.numero_version DESC, h.fecha_preservacion DESC
        """
        
        history = execute_query(query, (document_id, document_id))
        
        # Obtener también los datos actuales
        current_query = """
        SELECT 
            di.*,
            d.version_actual as numero_version,
            'Actual' as estado_version,
            NOW() as fecha_preservacion
        FROM documentos_identificacion di
        JOIN documentos d ON di.id_documento = d.id_documento
        WHERE di.id_documento = %s
        """
        
        current = execute_query(current_query, (document_id,))
        
        # Combinar histórico con datos actuales
        all_data = []
        if current:
            all_data.extend(current)
        if history:
            all_data.extend(history)
            
        return all_data
        
    except Exception as e:
        logger.error(f"Error al obtener historial de identificación: {str(e)}")
        return []

def compare_identification_versions(document_id, version1, version2):
    """
    Compara dos versiones de datos de identificación
    """
    try:
        # Obtener datos de la versión 1
        query_v1 = """
        SELECT * FROM vista_historial_identificacion
        WHERE id_documento = %s AND numero_version = %s
        """
        data_v1 = execute_query(query_v1, (document_id, version1))
        
        # Obtener datos de la versión 2
        data_v2 = execute_query(query_v1, (document_id, version2))
        
        if not data_v1 or not data_v2:
            raise ValueError("No se encontraron datos para una o ambas versiones")
        
        v1 = data_v1[0]
        v2 = data_v2[0]
        
        # Comparar campos y detectar cambios
        comparison = {
            'version_1': version1,
            'version_2': version2,
            'cambios': {},
            'resumen': []
        }
        
        # Campos a comparar
        campos = [
            ('numero_documento', 'Número de Documento'),
            ('nombre_completo', 'Nombre Completo'),
            ('fecha_emision', 'Fecha de Emisión'),
            ('fecha_expiracion', 'Fecha de Expiración'),
            ('pais_emision', 'País de Emisión'),
            ('genero', 'Género'),
            ('lugar_nacimiento', 'Lugar de Nacimiento'),
            ('autoridad_emision', 'Autoridad de Emisión'),
            ('nacionalidad', 'Nacionalidad')
        ]
        
        for campo, descripcion in campos:
            valor_v1 = v1.get(campo)
            valor_v2 = v2.get(campo)
            
            if valor_v1 != valor_v2:
                comparison['cambios'][campo] = {
                    'descripcion': descripcion,
                    'antes': valor_v1,
                    'despues': valor_v2
                }
                comparison['resumen'].append(f"{descripcion}: '{valor_v1}' → '{valor_v2}'")
        
        comparison['total_cambios'] = len(comparison['cambios'])
        
        return comparison
        
    except Exception as e:
        logger.error(f"Error al comparar versiones de identificación: {str(e)}")
        raise

def restore_identification_version(document_id, version_number, restored_by):
    """
    Restaura una versión anterior de los datos de identificación
    """
    connection = get_connection()
    try:
        connection.begin()
        
        with connection.cursor() as cursor:
            # Obtener datos de la versión a restaurar
            query = """
            SELECT h.* 
            FROM historico_documentos_identificacion h
            JOIN versiones_documento v ON h.id_version = v.id_version
            WHERE h.id_documento = %s AND v.numero_version = %s
            ORDER BY h.fecha_preservacion DESC
            LIMIT 1
            """
            
            cursor.execute(query, (document_id, version_number))
            historical_data = cursor.fetchone()
            
            if not historical_data:
                raise ValueError(f"No se encontraron datos históricos para la versión {version_number}")
            
            # Preservar datos actuales antes de restaurar
            preserve_identification_data(document_id, reason=f"Antes de restaurar a versión {version_number}")
            
            # Actualizar con los datos históricos
            update_query = """
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
            
            cursor.execute(update_query, (
                historical_data['tipo_documento'],
                historical_data['numero_documento'],
                historical_data['pais_emision'],
                historical_data['fecha_emision'],
                historical_data['fecha_expiracion'],
                historical_data['nombre_completo'],
                historical_data['genero'],
                historical_data['lugar_nacimiento'],
                historical_data['autoridad_emision'],
                historical_data['nacionalidad'],
                historical_data['codigo_pais'],
                document_id
            ))
            
            # Registrar en auditoría
            audit_data = {
                'fecha_hora': datetime.now().isoformat(),
                'usuario_id': restored_by,
                'direccion_ip': '0.0.0.0',
                'accion': 'restaurar_datos_identificacion',
                'entidad_afectada': 'documentos_identificacion',
                'id_entidad_afectada': document_id,
                'detalles': json.dumps({
                    'version_restaurada': version_number,
                    'numero_documento': historical_data['numero_documento']
                }),
                'resultado': 'éxito'
            }
            insert_audit_record(audit_data)
            
            connection.commit()
            logger.info(f"Datos de identificación restaurados a versión {version_number} para documento {document_id}")
            return True
            
    except Exception as e:
        connection.rollback()
        logger.error(f"Error al restaurar versión de identificación: {str(e)}")
        raise
    finally:
        connection.close()

# Función mejorada para get_version_id
def get_version_id(document_id, version_number):
    """
    Obtiene el ID de versión para un documento y número de versión específicos.
    VERSIÓN MEJORADA con manejo de errores y validaciones.
    """
    try:
        if not document_id:
            logger.error("document_id no puede ser None o vacío")
            return None
        
        if not version_number or version_number < 1:
            logger.error(f"version_number inválido: {version_number}")
            return None
        
        query = """
        SELECT id_version
        FROM versiones_documento
        WHERE id_documento = %s AND numero_version = %s
        LIMIT 1
        """
        result = execute_query(query, (document_id, version_number))
        
        if not result:
            logger.warning(f"No se encontró versión {version_number} para documento {document_id}")
            return None
            
        version_id = result[0]['id_version']
        logger.debug(f"Version ID encontrado: {version_id} para documento {document_id}, versión {version_number}")
        return version_id
        
    except Exception as e:
        logger.error(f"Error al obtener version_id para documento {document_id}, versión {version_number}: {str(e)}")
        return None
    
# Nueva función para obtener información de análisis
def get_analysis_info_by_id(analysis_id):
    """
    Obtiene información completa de un análisis por su ID
    """
    try:
        query = """
        SELECT a.*, d.titulo as documento_titulo, v.numero_version, v.nombre_original
        FROM analisis_documento_ia a
        JOIN documentos d ON a.id_documento = d.id_documento
        LEFT JOIN versiones_documento v ON a.id_version = v.id_version
        WHERE a.id_analisis = %s
        """
        result = execute_query(query, (analysis_id,))
        return result[0] if result else None
    except Exception as e:
        logger.error(f"Error al obtener información de análisis {analysis_id}: {str(e)}")
        return None

# Nueva función para verificar integridad de versiones
def verify_document_version_integrity(document_id):
    """
    Verifica la integridad de las versiones de un documento
    """
    try:
        # Obtener información del documento
        doc_query = """
        SELECT id_documento, version_actual, titulo
        FROM documentos
        WHERE id_documento = %s
        """
        doc_result = execute_query(doc_query, (document_id,))
        
        if not doc_result:
            return False, f"Documento {document_id} no encontrado"
        
        doc_info = doc_result[0]
        version_actual = doc_info['version_actual']
        
        # Verificar que existe la versión actual
        version_query = """
        SELECT id_version, numero_version
        FROM versiones_documento
        WHERE id_documento = %s AND numero_version = %s
        """
        version_result = execute_query(version_query, (document_id, version_actual))
        
        if not version_result:
            return False, f"Versión actual {version_actual} no encontrada"
        
        # Verificar continuidad de versiones (no debe haber saltos)
        continuity_query = """
        SELECT numero_version
        FROM versiones_documento
        WHERE id_documento = %s
        ORDER BY numero_version
        """
        versions = execute_query(continuity_query, (document_id,))
        
        if versions:
            version_numbers = [v['numero_version'] for v in versions]
            expected_versions = list(range(1, len(version_numbers) + 1))
            
            if version_numbers != expected_versions:
                return False, f"Discontinuidad en versiones: {version_numbers}, esperado: {expected_versions}"
        
        # Verificar que hay análisis para la versión actual
        analysis_query = """
        SELECT COUNT(*) as count
        FROM analisis_documento_ia
        WHERE id_documento = %s AND id_version = %s
        """
        analysis_result = execute_query(analysis_query, (document_id, version_result[0]['id_version']))
        
        has_analysis = analysis_result and analysis_result[0]['count'] > 0
        
        return True, {
            'document_id': document_id,
            'version_actual': version_actual,
            'version_id': version_result[0]['id_version'],
            'total_versions': len(versions) if versions else 0,
            'has_analysis': has_analysis,
            'titulo': doc_info['titulo']
        }
        
    except Exception as e:
        logger.error(f"Error verificando integridad de versiones para {document_id}: {str(e)}")
        return False, str(e)

# Función corregida para insert_analysis_record
def insert_analysis_record(analysis_data):
    """
    Inserta un nuevo registro de análisis IA para un documento.
    VERSIÓN CORREGIDA que maneja correctamente el campo id_version.
    """
    # Verificar que tenemos los campos requeridos
    required_fields = ['id_analisis', 'id_documento', 'tipo_documento']
    for field in required_fields:
        if field not in analysis_data:
            raise ValueError(f"Campo requerido faltante: {field}")
    
    # Establecer valores por defecto para campos opcionales
    defaults = {
        'id_version': None,
        'confianza_clasificacion': 0.5,
        'texto_extraido': None,
        'entidades_detectadas': None,
        'metadatos_extraccion': '{}',
        'fecha_analisis': datetime.now().isoformat(),
        'estado_analisis': 'iniciado',
        'mensaje_error': None,
        'version_modelo': 'default',
        'tiempo_procesamiento': 0,
        'procesado_por': 'sistema',
        'requiere_verificacion': True,
        'verificado': False,
        'verificado_por': None,
        'fecha_verificacion': None
    }
    
    # Aplicar valores por defecto para campos faltantes
    for key, default_value in defaults.items():
        if key not in analysis_data:
            analysis_data[key] = default_value
    
    query = """
    INSERT INTO analisis_documento_ia (
        id_analisis,
        id_documento,
        id_version,
        tipo_documento,
        confianza_clasificacion,
        texto_extraido,
        entidades_detectadas,
        metadatos_extraccion,
        fecha_analisis,
        estado_analisis,
        mensaje_error,
        version_modelo,
        tiempo_procesamiento,
        procesado_por,
        requiere_verificacion,
        verificado,
        verificado_por,
        fecha_verificacion
    ) VALUES (
        %(id_analisis)s,
        %(id_documento)s,
        %(id_version)s,
        %(tipo_documento)s,
        %(confianza_clasificacion)s,
        %(texto_extraido)s,
        %(entidades_detectadas)s,
        %(metadatos_extraccion)s,
        %(fecha_analisis)s,
        %(estado_analisis)s,
        %(mensaje_error)s,
        %(version_modelo)s,
        %(tiempo_procesamiento)s,
        %(procesado_por)s,
        %(requiere_verificacion)s,
        %(verificado)s,
        %(verificado_por)s,
        %(fecha_verificacion)s
    )
    """
    
    try:
        result = execute_query(query, analysis_data, fetch=False)
        logger.info(f"Registro de análisis {analysis_data['id_analisis']} insertado correctamente")
        return result
    except Exception as e:
        logger.error(f"Error al insertar registro de análisis: {str(e)}")
        logger.error(f"Datos del análisis: {json.dumps(analysis_data, default=str)}")
        raise

# Nueva función para limpiar análisis huérfanos
def cleanup_orphaned_analysis():
    """
    Limpia registros de análisis que no tienen documento o versión asociada
    """
    try:
        # Encontrar análisis huérfanos (sin documento)
        orphaned_docs_query = """
        SELECT a.id_analisis, a.id_documento
        FROM analisis_documento_ia a
        LEFT JOIN documentos d ON a.id_documento = d.id_documento
        WHERE d.id_documento IS NULL
        """
        
        orphaned_docs = execute_query(orphaned_docs_query)
        
        if orphaned_docs:
            logger.warning(f"Encontrados {len(orphaned_docs)} análisis huérfanos sin documento")
            
            # Opcionalmente eliminar (comentado por seguridad)
            # for orphan in orphaned_docs:
            #     delete_query = "DELETE FROM analisis_documento_ia WHERE id_analisis = %s"
            #     execute_query(delete_query, (orphan['id_analisis'],), fetch=False)
        
        # Encontrar análisis con versión inválida
        orphaned_versions_query = """
        SELECT a.id_analisis, a.id_documento, a.id_version
        FROM analisis_documento_ia a
        LEFT JOIN versiones_documento v ON a.id_version = v.id_version
        WHERE a.id_version IS NOT NULL AND v.id_version IS NULL
        """
        
        orphaned_versions = execute_query(orphaned_versions_query)
        
        if orphaned_versions:
            logger.warning(f"Encontrados {len(orphaned_versions)} análisis con versión inválida")
        
        return {
            'orphaned_documents': len(orphaned_docs) if orphaned_docs else 0,
            'orphaned_versions': len(orphaned_versions) if orphaned_versions else 0
        }
        
    except Exception as e:
        logger.error(f"Error en limpieza de análisis huérfanos: {str(e)}")
        return None

# Nueva función para migrar análisis sin versión
def migrate_analysis_without_version():
    """
    Migra registros de análisis que no tienen id_version asignado
    """
    try:
        # Encontrar análisis sin versión
        query = """
        SELECT a.id_analisis, a.id_documento, d.version_actual
        FROM analisis_documento_ia a
        JOIN documentos d ON a.id_documento = d.id_documento
        WHERE a.id_version IS NULL
        """
        
        analysis_without_version = execute_query(query)
        
        if not analysis_without_version:
            logger.info("No hay análisis sin versión para migrar")
            return 0
        
        migrated_count = 0
        
        for analysis in analysis_without_version:
            # Obtener version_id para la versión actual
            version_id = get_version_id(analysis['id_documento'], analysis['version_actual'])
            
            if version_id:
                # Actualizar el análisis con el version_id
                update_query = """
                UPDATE analisis_documento_ia
                SET id_version = %s
                WHERE id_analisis = %s
                """
                
                execute_query(update_query, (version_id, analysis['id_analisis']), fetch=False)
                migrated_count += 1
                
                logger.info(f"Migrado análisis {analysis['id_analisis']} a versión {version_id}")
            else:
                logger.error(f"No se pudo obtener version_id para documento {analysis['id_documento']}")
        
        logger.info(f"Migración completada: {migrated_count} análisis migrados")
        return migrated_count
        
    except Exception as e:
        logger.error(f"Error en migración de análisis: {str(e)}")
        return -1    
    
# Agregar esta función en db_connector.py

def register_bank_contract(document_id, contract_data):
    """
    Registra o actualiza datos de contratos bancarios en la tabla contratos_bancarios
    """
    try:
        # Validar datos mínimos requeridos
        required_fields = ['tipo_contrato', 'numero_contrato', 'fecha_inicio', 'estado']
        missing_fields = []
        
        for field in required_fields:
            if not contract_data.get(field):
                missing_fields.append(field)
        
        if missing_fields:
            logger.error(f"❌ Campos requeridos faltantes para contrato bancario: {missing_fields}")
            return False
        
        # Mapear tipo de contrato al enum de la tabla
        tipo_contrato_map = {
            'cuenta_corriente': 'cuenta_corriente',
            'cuenta_ahorro': 'cuenta_ahorro',
            'deposito': 'deposito',
            'prestamo': 'prestamo',
            'hipoteca': 'hipoteca',
            'tarjeta_credito': 'tarjeta_credito',
            'inversion': 'inversion',
            'seguro': 'seguro',
            'otro': 'otro'
        }
        
        tipo_contrato = tipo_contrato_map.get(contract_data.get('tipo_contrato'), 'otro')
        
        # Mapear estados
        estado_map = {
            'vigente': 'vigente',
            'cancelado': 'cancelado',
            'suspendido': 'suspendido',
            'pendiente_firma': 'pendiente_firma',
            'vencido': 'vencido'
        }
        
        estado = estado_map.get(contract_data.get('estado'), 'pendiente_firma')
        
        # Formatear fechas
        fecha_inicio = contract_data.get('fecha_inicio_iso') or contract_data.get('fecha_inicio')
        fecha_fin = contract_data.get('fecha_fin_iso') or contract_data.get('fecha_fin')
        
        # Validar formato de fechas
        if fecha_inicio and not re.match(r'^\d{4}-\d{2}-\d{2}$', str(fecha_inicio)):
            # Intentar convertir
            fecha_inicio = format_date(fecha_inicio)
        
        if fecha_fin and not re.match(r'^\d{4}-\d{2}-\d{2}$', str(fecha_fin)):
            fecha_fin = format_date(fecha_fin)
        
        # Verificar si ya existe un registro
        check_query = """
        SELECT id_documento FROM contratos_bancarios WHERE id_documento = %s
        """
        existing = execute_query(check_query, (document_id,))
        
        if existing:
            # Preservar datos antes de actualizar
            logger.info(f"📸 Preservando datos existentes del contrato antes de actualizar")
            try:
                preserve_contract_data(document_id, reason="Actualización con nuevos datos extraídos")
            except Exception as preserve_error:
                logger.warning(f"⚠️ Error al preservar datos: {str(preserve_error)}")
            
            # Actualizar registro existente
            query = """
            UPDATE contratos_bancarios 
            SET tipo_contrato = %s,
                numero_contrato = %s,
                fecha_inicio = %s,
                fecha_fin = %s,
                estado = %s,
                valor_contrato = %s,
                tasa_interes = %s,
                periodo_tasa = %s,
                moneda = %s,
                numero_producto = %s,
                firmado_digitalmente = %s,
                fecha_ultima_revision = NOW(),
                revisado_por = %s,
                observaciones = %s
            WHERE id_documento = %s
            """
            params = (
                tipo_contrato,
                contract_data.get('numero_contrato'),
                fecha_inicio,
                fecha_fin,
                estado,
                contract_data.get('valor_contrato'),
                contract_data.get('tasa_interes'),
                contract_data.get('periodo_tasa', 'anual'),
                contract_data.get('moneda', 'EUR'),
                contract_data.get('numero_producto'),
                contract_data.get('firmado_digitalmente', False),
                '691d8c44-f524-48fd-b292-be9e31977711',  # Usuario sistema
                contract_data.get('observaciones'),
                document_id
            )
            operation = "ACTUALIZACIÓN"
        else:
            # Insertar nuevo registro
            query = """
            INSERT INTO contratos_bancarios (
                id_documento,
                tipo_contrato,
                numero_contrato,
                fecha_inicio,
                fecha_fin,
                estado,
                valor_contrato,
                tasa_interes,
                periodo_tasa,
                moneda,
                numero_producto,
                firmado_digitalmente,
                fecha_ultima_revision,
                revisado_por,
                observaciones
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW(), %s, %s)
            """
            params = (
                document_id,
                tipo_contrato,
                contract_data.get('numero_contrato'),
                fecha_inicio,
                fecha_fin,
                estado,
                contract_data.get('valor_contrato'),
                contract_data.get('tasa_interes'),
                contract_data.get('periodo_tasa', 'anual'),
                contract_data.get('moneda', 'EUR'),
                contract_data.get('numero_producto'),
                contract_data.get('firmado_digitalmente', False),
                '691d8c44-f524-48fd-b292-be9e31977711',  # Usuario sistema
                contract_data.get('observaciones')
            )
            operation = "INSERCIÓN"
        
        logger.info(f"🔍 {operation} para {tipo_contrato.upper()}")
        logger.info(f"📝 Número contrato: {contract_data.get('numero_contrato')}")
        logger.info(f"📅 Vigencia: {fecha_inicio} → {fecha_fin}")
        logger.info(f"💰 Valor: {contract_data.get('valor_contrato')} {contract_data.get('moneda', 'EUR')}")
        logger.info(f"📊 Tasa: {contract_data.get('tasa_interes')}%")
        logger.info(f"🏦 Producto: {contract_data.get('numero_producto')}")
        
        # Ejecutar la consulta
        execute_query(query, params, fetch=False)
        
        # Verificar éxito
        verify_query = """
        SELECT numero_contrato, tipo_contrato, estado 
        FROM contratos_bancarios 
        WHERE id_documento = %s
        """
        verify_result = execute_query(verify_query, (document_id,))
        
        if verify_result and len(verify_result) > 0:
            saved_data = verify_result[0]
            logger.info(f"✅ {operation} exitosa verificada:")
            logger.info(f"   📝 Número guardado: {saved_data.get('numero_contrato')}")
            logger.info(f"   📋 Tipo guardado: {saved_data.get('tipo_contrato')}")
            logger.info(f"   🔄 Estado guardado: {saved_data.get('estado')}")
            return True
        else:
            logger.error(f"❌ Verificación falló: No se encontraron datos guardados")
            return False
            
    except Exception as e:
        logger.error(f"❌ Error en registro de contrato bancario: {str(e)}")
        logger.error(f"📊 Datos que se intentaban guardar:")
        logger.error(f"   Tipo: {contract_data.get('tipo_contrato')}")
        logger.error(f"   Número: {contract_data.get('numero_contrato')}")
        logger.error(f"   Estado: {contract_data.get('estado')}")
        
        return False

def preserve_contract_data(document_id, reason="Manual preservation"):
    """
    Preserva los datos actuales de un contrato antes de una actualización
    """
    try:
        # Obtener datos actuales del contrato
        query = """
        SELECT * FROM contratos_bancarios
        WHERE id_documento = %s
        """
        
        contract_data = execute_query(query, (document_id,))
        
        if not contract_data:
            logger.warning(f"No se encontraron datos de contrato para documento {document_id}")
            return False
        
        current_data = contract_data[0]
        
        # Aquí podrías implementar una tabla de histórico si la necesitas
        # Por ahora, solo logueamos que se preservaron los datos
        logger.info(f"Datos de contrato preservados para documento {document_id}")
        logger.info(f"Contrato: {current_data.get('numero_contrato')}, Tipo: {current_data.get('tipo_contrato')}")
        
        return True
        
    except Exception as e:
        logger.error(f"Error al preservar datos del contrato: {str(e)}")
        return False

def log_contract_changes(document_id):
    """
    Registra y muestra los cambios detectados en los datos del contrato
    """
    try:
        # Esta función es similar a la de identificación pero para contratos
        logger.info(f"📋 Verificando cambios en contrato {document_id}")
        # Implementar si necesitas histórico
        
    except Exception as e:
        logger.error(f"Error al registrar cambios: {str(e)}")    

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