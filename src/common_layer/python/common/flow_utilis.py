import os
import logging
import json
from common.db_connector import execute_query, log_document_processing_start, log_document_processing_end, generate_uuid

logger = logging.getLogger()
logger.setLevel(os.environ.get('LOG_LEVEL', 'INFO'))


def actualizar_estado_cliente_revision(client_id):
    """
    Actualiza el estado del cliente y su instancia de flujo a 'en_revision_kyc'
    si el estado actual es 'incompleto'
    """
    logger.info(f"🔄 ACTUALIZANDO estado del cliente {client_id} para revisión KYC")
    
    try:
        # Verificar estado actual del cliente
        check_client_state_query = """
        SELECT estado_documental 
        FROM clientes 
        WHERE id_cliente = %s
        """
        client_state_result = execute_query(check_client_state_query, (client_id,))
        
        if not client_state_result:
            logger.warning(f"⚠️ Cliente {client_id} no encontrado")
            return False
            
        current_state = client_state_result[0]['estado_documental']
        logger.info(f"   📊 Estado actual del cliente: {current_state}")
        
        if current_state == 'iniciado' or current_state == 'incompleto':
            # Actualizar cliente a 'en_revision_kyc'
            update_client_query = """
            UPDATE clientes 
            SET estado_documental = 'en_revision_kyc',
                fecha_ultima_actividad = NOW()
            WHERE id_cliente = %s
            """
            
            execute_query(update_client_query, (client_id,), fetch=False)
            logger.info(f"✅ Cliente actualizado: 'incompleto' → 'en_revision_kyc'")
            
            # Actualizar instancia de flujo cliente
            update_flujo_cliente_query = """
            UPDATE instancias_flujo_cliente
            SET estado_actual = 'en_revision_kyc'
            WHERE id_cliente = %s
            """
            
            execute_query(update_flujo_cliente_query, (client_id,), fetch=False)
            logger.info(f"✅ Instancia flujo cliente actualizada a 'en_revision_kyc'")
            logger.info(f"   📅 Fecha última actividad actualizada")
            
            return True
        else:
            logger.info(f"✅ Cliente ya está en estado '{current_state}', no requiere actualización")
            return False
            
    except Exception as e:
        logger.error(f"💥 ERROR al actualizar estado del cliente {client_id}: {str(e)}")
        return False

def crear_instancia_flujo_documento(document_id):
    """
    Crea instancia de flujo para documento individual y notifica al Oficial KYC
    """
    logger.info(f"🚀 INICIANDO creación de instancia de flujo para documento: {document_id}")
    
    try:
        # PASO 1: Obtener información del documento y cliente
        logger.info("📋 PASO 1: Obteniendo información del documento y cliente")
        doc_query = """
        SELECT d.id_documento, d.id_tipo_documento, d.titulo,
               dc.id_cliente, c.nombre_razon_social, c.codigo_cliente
        FROM documentos d
        JOIN documentos_clientes dc ON d.id_documento = dc.id_documento
        JOIN clientes c ON dc.id_cliente = c.id_cliente
        WHERE d.id_documento = %s
        """
        
        logger.debug(f"🔍 Ejecutando consulta documento: {doc_query}")
        logger.debug(f"🔍 Parámetros: document_id={document_id}")
        
        doc_result = execute_query(doc_query, (document_id,))
        
        if not doc_result:
            logger.error(f"❌ PASO 1 FALLIDO: Documento {document_id} no encontrado en la base de datos")
            return None
        
        doc_info = doc_result[0]
        client_id = doc_info['id_cliente']
        
        logger.info(f"✅ PASO 1 COMPLETADO: Documento encontrado")
        logger.info(f"   📄 ID Documento: {doc_info['id_documento']}")
        logger.info(f"   📄 Título: {doc_info['titulo']}")
        logger.info(f"   👤 Cliente ID: {client_id}")
        logger.info(f"   👤 Cliente: {doc_info['nombre_razon_social']} ({doc_info['codigo_cliente']})")
        
        # PASO 2: Obtener flujo KYC del cliente
        logger.info(f"🔄 PASO 2: Obteniendo flujo KYC del cliente {client_id}")
        flujo_query = """
        SELECT id_instancia, id_flujo
        FROM instancias_flujo_cliente
        WHERE id_cliente = %s
        ORDER BY fecha_inicio DESC
        LIMIT 1
        """
        
        logger.debug(f"🔍 Ejecutando consulta flujo: {flujo_query}")
        logger.debug(f"🔍 Parámetros: client_id={client_id}")
        
        flujo_result = execute_query(flujo_query, (client_id,))
        
        if not flujo_result:
            logger.warning(f"⚠️ PASO 2 FALLIDO: No se encontró flujo KYC activo para cliente {client_id}")
            logger.warning(f"   💡 Posibles causas: Cliente sin flujo KYC iniciado o flujo completado")
            return None
        
        flujo_info = flujo_result[0]
        logger.info(f"✅ PASO 2 COMPLETADO: Flujo KYC encontrado")
        logger.info(f"   🔄 ID Instancia Flujo: {flujo_info['id_instancia']}")
        logger.info(f"   🔄 ID Flujo: {flujo_info['id_flujo']}")

        # PASO 2.5: Actualizar estado del cliente a 'en_revision_kyc' si está en 'incompleto'
        logger.info(f"🔄 PASO 2.5: Verificando y actualizando estado del cliente")
        actualizar_estado_cliente_revision(client_id)
        logger.info(f"✅ PASO 2.5 COMPLETADO: Verificación de estado finalizada")
 
        # PASO 3: Verificar si ya existe instancia para este documento
        logger.info(f"🔍 PASO 3: Verificando si ya existe instancia para documento {document_id}")
        existing_query = """
        SELECT id_instancia, estado_actual
        FROM instancias_flujo_documento
        WHERE id_documento = %s
        """
        
        logger.debug(f"🔍 Ejecutando consulta existencia: {existing_query}")
        logger.debug(f"🔍 Parámetros: document_id={document_id}")
        
        existing = execute_query(existing_query, (document_id,))
        
        if existing:
            return _procesar_instancia_existente(existing[0], document_id, doc_info)
        
        logger.info(f"✅ PASO 3 COMPLETADO: No existe instancia previa, procediendo a crear nueva")
        return _crear_nueva_instancia(document_id, client_id, flujo_info, doc_info)
        
    except Exception as e:
        logger.error(f"💥 ERROR CRÍTICO en creación de instancia flujo documento")
        logger.error(f"   📄 Documento ID: {document_id}")
        logger.error(f"   🔥 Error: {str(e)}")
        logger.error(f"   📍 Tipo de error: {type(e).__name__}")
        
        # Log adicional para debugging
        import traceback
        logger.error(f"   📋 Traceback completo:\n{traceback.format_exc()}")
        
        return None

def _procesar_instancia_existente(existing_info, document_id, doc_info):
    """
    Procesa una instancia de flujo documento existente actualizando su estado
    """
    existing_id = existing_info['id_instancia']
    estado_anterior = existing_info['estado_actual']
    oficial_kyc_id = '8fc64015-c76a-4bf3-ac23-aeac661ef989'
    
    logger.info(f"✅ PASO 3 COMPLETADO: Ya existe instancia de flujo para documento {document_id}")
    logger.info(f"   🔄 ID Instancia Existente: {existing_id}")
    logger.info(f"   📊 Estado anterior: {estado_anterior}")
    
    # PASO 4A: Actualizar estado de instancia existente a 'recibido'
    logger.info("🔄 PASO 4A: Actualizando estado de instancia existente a 'recibido'")
    update_query = """
    UPDATE instancias_flujo_documento 
    SET estado_actual = %s, 
        asignado_a = %s,
        prioridad = %s,
        fecha_inicio = NOW()
    WHERE id_instancia = %s
    """
    
    update_params = ('recibido', oficial_kyc_id, 'media', existing_id)
    
    logger.debug(f"🔍 Ejecutando actualización: {update_query}")
    logger.debug(f"🔍 Parámetros: {update_params}")
    
    execute_query(update_query, update_params, fetch=False)
    
    logger.info(f"✅ PASO 4A COMPLETADO: Estado actualizado exitosamente")
    logger.info(f"   📊 Estado anterior: {estado_anterior} → Estado actual: 'recibido'")
    logger.info(f"   👮 Reasignado a: {oficial_kyc_id}")
    logger.info(f"   ⚡ Prioridad: 'media'")
    
    # PASO 4B: Registrar actividad en histórico para trazabilidad
    logger.info("📋 PASO 4B: Registrando actividad en histórico para instancia existente")
    historico_id = generate_uuid()
    
    historico_query = """
    INSERT INTO historico_flujo (
        id_historico, id_instancia, estado_destino_id,
        usuario_id, comentario, estado_origen_id, fecha_transicion
    ) VALUES (%s, %s, %s, %s, %s, %s, NOW())
    """
    
    comentario = f'Documento {document_id} vuelto a procesar - Estado actualizado de "{estado_anterior}" a "recibido"'
    
    execute_query(historico_query, (
        historico_id, existing_id, 'recibido',
        '691d8c44-f524-48fd-b292-be9e31977711', 
        comentario,
        estado_anterior
    ), fetch=False)
    
    logger.info(f"✅ PASO 4B COMPLETADO: Histórico registrado: {historico_id}")
    logger.info(f"   📝 Comentario: {comentario}")
    
    # PASO 5: Crear notificación para Oficial KYC
    _crear_notificacion_kyc(existing_id, oficial_kyc_id, doc_info, document_id, estado_anterior)
    
    # RESUMEN FINAL PARA INSTANCIA EXISTENTE
    logger.info(f"🎉 PROCESO COMPLETADO EXITOSAMENTE (INSTANCIA ACTUALIZADA)")
    logger.info(f"   📄 Documento procesado: {document_id} - {doc_info['titulo']}")
    logger.info(f"   👤 Cliente: {doc_info['nombre_razon_social']} ({doc_info['codigo_cliente']})")
    logger.info(f"   🔄 Instancia actualizada: {existing_id}")
    logger.info(f"   📊 Cambio de estado: {estado_anterior} → 'recibido'")
    logger.info(f"   👮 Reasignado a: {oficial_kyc_id}")
    
    return existing_id

def _crear_nueva_instancia(document_id, client_id, flujo_info, doc_info):
    """
    Crea una nueva instancia de flujo documento
    """
    # PASO 4: Crear instancia de flujo documento
    logger.info("📝 PASO 4: Creando nueva instancia de flujo documento")
    id_instancia_doc = generate_uuid()
    oficial_kyc_id = '8fc64015-c76a-4bf3-ac23-aeac661ef989'
    
    logger.info(f"   🆔 ID Nueva Instancia: {id_instancia_doc}")
    logger.info(f"   👮 Asignado a Oficial KYC: {oficial_kyc_id}")
    
    insert_query = """
    INSERT INTO instancias_flujo_documento (
        id_instancia, id_documento, id_cliente, id_flujo,
        estado_actual, asignado_a, prioridad, fecha_inicio
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, NOW())
    """
    
    insert_params = (
        id_instancia_doc, document_id, client_id, flujo_info['id_flujo'],
        'recibido', oficial_kyc_id, 'media'
    )
    
    logger.debug(f"🔍 Ejecutando inserción instancia: {insert_query}")
    logger.debug(f"🔍 Parámetros: {insert_params}")
    
    execute_query(insert_query, insert_params, fetch=False)
    
    logger.info(f"✅ PASO 4 COMPLETADO: Instancia de flujo documento creada exitosamente")
    logger.info(f"   📊 Estado inicial: 'recibido'")
    logger.info(f"   ⚡ Prioridad: 'media'")
    
    # PASO 5: Registrar actividad en histórico
    logger.info("📋 PASO 5: Registrando actividad inicial en histórico")
    historico_id = generate_uuid()
    
    historico_query = """
    INSERT INTO historico_flujo (
        id_historico, id_instancia, estado_destino_id,
        usuario_id, comentario, estado_origen_id, fecha_transicion
    ) VALUES (%s, %s, %s, %s, %s, %s, NOW())
    """
    
    execute_query(historico_query, (
        historico_id, id_instancia_doc, 'recibido',
        '691d8c44-f524-48fd-b292-be9e31977711', 
        f'Nueva instancia creada para documento {document_id}',
        'iniciado'
    ), fetch=False)
    
    logger.info(f"✅ PASO 5 COMPLETADO: Histórico registrado: {historico_id}")
    
    # PASO 6: Crear notificación para Oficial KYC
    _crear_notificacion_kyc(id_instancia_doc, oficial_kyc_id, doc_info, document_id)
    
    # RESUMEN FINAL
    logger.info(f"🎉 PROCESO COMPLETADO EXITOSAMENTE (NUEVA INSTANCIA)")
    logger.info(f"   📄 Documento procesado: {document_id} - {doc_info['titulo']}")
    logger.info(f"   👤 Cliente: {doc_info['nombre_razon_social']} ({doc_info['codigo_cliente']})")
    logger.info(f"   🔄 Instancia creada: {id_instancia_doc}")
    logger.info(f"   👮 Asignado a: {oficial_kyc_id}")
    
    return id_instancia_doc

def _crear_notificacion_kyc(id_instancia, oficial_kyc_id, doc_info, document_id, estado_anterior=None):
    """
    Crea notificación para el Oficial KYC
    """
    logger.info("📧 Creando notificación para Oficial KYC")
    id_notificacion = generate_uuid()
    
    logger.info(f"   🆔 ID Notificación: {id_notificacion}")
    
    notif_query = """
    INSERT INTO notificaciones_flujo (
        id_notificacion, id_instancia_flujo, id_usuario_destino,
        tipo_notificacion, titulo, mensaje, urgencia,
        datos_contextuales
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """
    
    if estado_anterior:
        titulo = f"Documento actualizado: {doc_info['titulo']}"
        mensaje = f"Documento del cliente {doc_info['codigo_cliente']} - {doc_info['nombre_razon_social']} ha sido actualizado y requiere revisión KYC"
        datos_contextuales = {
            'id_cliente': doc_info['id_cliente'],
            'id_documento': document_id,
            'nombre_cliente': doc_info['nombre_razon_social'],
            'codigo_cliente': doc_info['codigo_cliente'],
            'titulo_documento': doc_info['titulo'],
            'estado_anterior': estado_anterior,
            'estado_actual': 'recibido'
        }
    else:
        titulo = f"Documento recibido: {doc_info['titulo']}"
        mensaje = f"Nuevo documento del cliente {doc_info['codigo_cliente']} - {doc_info['nombre_razon_social']} requiere revisión KYC"
        datos_contextuales = {
            'id_cliente': doc_info['id_cliente'],
            'id_documento': document_id,
            'nombre_cliente': doc_info['nombre_razon_social'],
            'codigo_cliente': doc_info['codigo_cliente'],
            'titulo_documento': doc_info['titulo']
        }
    
    notif_params = (
        id_notificacion, id_instancia, oficial_kyc_id,
        'tarea_asignada', titulo, mensaje, 'media',
        json.dumps(datos_contextuales)
    )
    
    logger.info(f"   📧 Título: {titulo}")
    logger.info(f"   📧 Mensaje: {mensaje}")
    logger.info(f"   📧 Destinatario: {oficial_kyc_id}")
    logger.debug(f"🔍 Ejecutando inserción notificación: {notif_query}")
    logger.debug(f"🔍 Parámetros: {notif_params}")
    logger.debug(f"🔍 Datos contextuales: {json.dumps(datos_contextuales, indent=2)}")
    
    execute_query(notif_query, notif_params, fetch=False)
    
    logger.info(f"✅ Notificación creada exitosamente: {id_notificacion}")
    
    return id_notificacion