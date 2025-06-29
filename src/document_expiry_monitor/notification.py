import json
import logging
import boto3
import os
from datetime import datetime, timedelta
import urllib.parse

# Configurar logger
logger = logging.getLogger()

# Configuración
SNS_TOPIC_ARN = os.environ.get('NOTIFICATION_TOPIC_ARN')
EMAIL_TEMPLATE_BUCKET = os.environ.get('EMAIL_TEMPLATE_BUCKET')
EMAIL_TEMPLATE_KEY_PREFIX = os.environ.get('EMAIL_TEMPLATE_KEY_PREFIX', 'templates/email/')
SOURCE_EMAIL = os.environ.get('SOURCE_EMAIL', 'notify@softwarefactory.cibernetica.xyz')
BASE_PORTAL_URL = os.environ.get('PORTAL_BASE_URL', 'https://main.d2ohqmd7t2mj86.amplifyapp.com/')
 

# Inicializar clientes AWS
sns_client = boto3.client('sns')
s3_client = boto3.client('s3')
ses_client = boto3.client('ses')

# Importar db
from common.db_connector import (
        execute_query,
        generate_uuid
)

def send_notification(client, document, days_threshold):
    """
    Envía notificación sobre documento por vencer
    
    Args:
        client: Información del cliente
        document: Documento por vencer
        days_threshold: Días hasta el vencimiento
        
    Returns:
        Boolean indicando si la notificación se envió correctamente
    """
    try:
        # Determinar tipo de notificación según umbral
        if days_threshold == 30:
            notification_type = 'aviso_inicial'
        elif days_threshold == 15:
            notification_type = 'recordatorio'
        elif days_threshold == 5:
            notification_type = 'urgente'
        else:
            notification_type = 'general'
        
        # Preparar datos para la notificación
        notification_data = {
            'client': {
                'id': client['id_cliente'],
                'name': client['nombre_razon_social'],
                'segment': client.get('segmento_bancario', 'general'),
                'contact_data': client.get('datos_contacto', {})
            },
            'document': {
                'id': document['id_documento'],
                'type': document['nombre_tipo'],
                'title': document.get('titulo', 'Documento'),
                'expiry_date': document['fecha_expiracion'].isoformat(),
                'days_to_expiry': days_threshold
            },
            'notification': {
                'type': notification_type,
                'timestamp': datetime.now().isoformat(),
                'renewal_request_id': document.get('id_solicitud')
            }
        }
        
        # Obtener destinatarios de la notificación
        recipients = get_notification_recipients(client, document)
        logger.info(f"Destinatarios para notificación: {json.dumps(recipients)}")
        
        # Intentar enviar por SES primero (para HTML formateado)
        if recipients.get('email'):
            sent_ses = False
            try:
                # Generar contenido de email
                html_content = generate_email_content(notification_data, notification_type)
                plain_text = generate_plain_text_content(notification_data, notification_type)
                
                # Para correos HTML, necesitamos especificar el formato correctamente
                email_message = {
                    'Subject': {
                        'Data': f"Documento próximo a vencer en {days_threshold} días",
                        'Charset': 'UTF-8'
                    },
                    'Body': {
                        'Html': {
                            'Data': html_content,
                            'Charset': 'UTF-8'
                        },
                        'Text': {
                            'Data': plain_text,
                            'Charset': 'UTF-8'
                        }
                    }
                }
                
                # Enviar a cada destinatario
                for recipient in recipients['email']:
                    # Enviar correo formateado HTML
                    ses_response = ses_client.send_email(
                        Source=SOURCE_EMAIL,  # Debe ser una dirección verificada en SES
                        Destination={'ToAddresses': [recipient]},
                        Message=email_message
                    )
                    logger.info(f"Correo HTML enviado a {recipient} mediante SES: {ses_response['MessageId']}")
                
                sent_ses = True
            except Exception as ses_error:
                logger.error(f"Error enviando por SES: {str(ses_error)}. Fallback a SNS.")
        
            # Si el envío por SES fue exitoso, no necesitamos usar SNS para email
            if sent_ses:
                # Si hay destinatarios SMS, enviarlos a través de SNS
                if recipients.get('sms'):
                    # Enviar sólo por SMS a través de SNS
                    try:
                        sms_message = generate_sms_content(notification_data, notification_type)
                        for phone in recipients['sms']:
                            sns_client.publish(
                                PhoneNumber=phone,
                                Message=sms_message
                            )
                    except Exception as sms_error:
                        logger.error(f"Error enviando SMS: {str(sms_error)}")
                
                return True
        
        # Fallback: Usar SNS con formato multicanal (para caso en que SES falle o no haya destinatarios email)
        message = {
            'default': json.dumps({
                'message': f"Documento próximo a vencer en {days_threshold} días",
                'data': notification_data
            }),
            'email': generate_email_content(notification_data, notification_type),
            'sms': generate_sms_content(notification_data, notification_type),
            'http': json.dumps(notification_data)
        }
        
        # Publicar mensaje en SNS con formato multicanal
        response = sns_client.publish(
            TopicArn=SNS_TOPIC_ARN,
            Message=json.dumps(message),
            MessageStructure='json',
            MessageAttributes={
                'NotificationType': {
                    'DataType': 'String',
                    'StringValue': notification_type
                },
                'ClientId': {
                    'DataType': 'String',
                    'StringValue': client['id_cliente']
                },
                'DocumentType': {
                    'DataType': 'String',
                    'StringValue': document['nombre_tipo']
                }
            }
        )
        
        logger.info(f"Notificación enviada mediante SNS: {response['MessageId']}")
        return True
        
    except Exception as e:
        logger.error(f"Error enviando notificación: {str(e)}")
        return False

def get_notification_recipients(client, document):
    """
    Determina los destinatarios de la notificación según cliente y documento
    
    Args:
        client: Datos del cliente
        document: Documento por vencer
        
    Returns:
        Dict con destinatarios por canal
    """
    recipients = {
        'email': [],
        'sms': [],
        'push': []
    }
    
    # Verificar que client no sea None antes de continuar
    if not client:
        logger.warning("Cliente es None, no se pueden determinar destinatarios")
        return recipients
    
    # Añadir contactos del cliente según preferencias
    contact_data = client.get('datos_contacto') or {}
    if not isinstance(contact_data, dict):
        contact_data = {}
    
    preferences = client.get('preferencias_comunicacion') or {}
    if not isinstance(preferences, dict):
        preferences = {}
    
    # Email del cliente si existe y tiene preferencia
    if isinstance(contact_data, dict) and 'email' in contact_data and preferences.get('email', True):
        recipients['email'].append(contact_data['email'])
    
    # Teléfono del cliente para SMS si existe y tiene preferencia
    if isinstance(contact_data, dict) and 'telefono' in contact_data and preferences.get('sms', False):
        recipients['sms'].append(contact_data['telefono'])
    
    # Añadir gestor principal si existe
    if client.get('gestor_principal_id'):
        # Aquí se implementaría lógica para obtener contactos del gestor
        # recipients['email'].append(gestor_email)
        pass
    
    # Si no hay destinatarios configurados, usar correo de prueba
    if not recipients['email'] and not recipients['sms']:
        recipients['email'].append('edwin.penalba@cibernetica.net')  # Para pruebas
    
    return recipients

def generate_email_content(notification_data, notification_type):
    """
    Genera contenido de email basado en plantillas
    
    Args:
        notification_data: Datos para la notificación
        notification_type: Tipo de notificación
        
    Returns:
        String con contenido HTML del email
    """
    # En una implementación completa, se cargaría una plantilla de S3
    # y se renderizaría con los datos específicos
    
    template_key = f"{EMAIL_TEMPLATE_KEY_PREFIX}expiry_{notification_type}.html"
    
    # Generar link personalizado con parámetros
    client_id = notification_data['client']['id']
    document_type = notification_data['document']['type']
    document_id = notification_data['document']['id']

    # Construir URL con parámetros
    renewal_link = build_renewal_link(client_id, document_type, document_id)

    try:
        # Intentar cargar plantilla desde S3
        response = s3_client.get_object(
            Bucket=EMAIL_TEMPLATE_BUCKET,
            Key=template_key
        )
        template_content = response['Body'].read().decode('utf-8')
        
        # Reemplazo directo de variables - más simple y efectivo
        replacements = {
            "{{client_name}}": notification_data['client']['name'],
            "{{document_title}}": notification_data['document']['type'],
            "{{document_expiry_date}}": notification_data['document']['expiry_date'],
            "{{document_days_to_expiry}}": str(notification_data['document']['days_to_expiry']),
            "{{client_segment}}": notification_data['client'].get('segment', 'General'),
            "{{renewal_link}}": renewal_link
        }
        
        # Aplicar reemplazos
        email_content = template_content
        for placeholder, value in replacements.items():
            email_content = email_content.replace(placeholder, str(value))
            
    except Exception as e:
        # Fallback a plantilla básica si hay error
        logger.warning(f"Error cargando plantilla de email: {str(e)}")
        
        client_name = notification_data['client']['name']
        doc_type = notification_data['document']['type']
        expiry_date = notification_data['document']['expiry_date']
        days = notification_data['document']['days_to_expiry']
        
        email_content = f"""
        <html>
        <body>
            <h2>Documento próximo a vencer</h2>
            <p>Estimado/a {client_name},</p>
            <p>Le informamos que su documento <strong>{doc_type}</strong> vencerá en <strong>{days} días</strong> ({expiry_date}).</p>
            <p>Por favor, renueve este documento a la brevedad para mantener su expediente actualizado.</p>
            <p>Saludos cordiales,<br/>Su Entidad Bancaria</p>
        </body>
        </html>
        """
    
    return email_content

def generate_plain_text_content(notification_data, notification_type):
    """
    Genera versión texto plano del email para clientes que no pueden ver HTML
    
    Args:
        notification_data: Datos para la notificación
        notification_type: Tipo de notificación
        
    Returns:
        String con contenido del texto plano
    """
    client_name = notification_data['client']['name']
    doc_type = notification_data['document']['title']
    expiry_date = notification_data['document']['expiry_date']
    days = notification_data['document']['days_to_expiry']
    
    return f"""
    Recordatorio Importante - Documento por Vencer
    
    Estimado/a {client_name},
    
    Le recordamos que su {doc_type} vencerá en {days} días ({expiry_date}).
    
    Este es un recordatorio de que necesita actualizar su documentación próximamente. 
    La no renovación a tiempo podría afectar sus operaciones bancarias y el cumplimiento 
    de requisitos regulatorios.
    
    Detalles del documento:
    - Tipo de documento: {doc_type}
    - Fecha de vencimiento: {expiry_date}
    - Días hasta vencimiento: {days}
    
    Si ya ha renovado este documento, por favor actualícelo en nuestra plataforma.
    
    Atentamente,
    Equipo de Gestión Documental
    Banco Ejemplo
    """

def generate_sms_content(notification_data, notification_type):
    """
    Genera contenido de SMS
    
    Args:
        notification_data: Datos para la notificación
        notification_type: Tipo de notificación
        
    Returns:
        String con contenido del SMS
    """
    client_name = notification_data['client']['name'].split()[0]  # Solo primer nombre
    days = notification_data['document']['days_to_expiry']
    doc_type = notification_data['document']['title']
    
    # Mensaje SMS conciso según tipo de notificación
    if notification_type == 'urgente':
        return f"URGENTE: {client_name}, su {doc_type} vence en {days} días. Renuévelo inmediatamente para evitar inconvenientes."
    else:
        return f"{client_name}, su {doc_type} vence en {days} días. Por favor renuévelo pronto."

def build_renewal_link(client_id, document_type, document_id=None):
    """
    Construye un link personalizado para renovación de documentos
    
    Args:
        client_id: ID del cliente
        document_type: Tipo de documento
        document_id: ID del documento (opcional)
        
    Returns:
        String con la URL completa
    """
    
    ## Generar token de sesión
    session_id = generate_uuid()

    # Parámetros base
    params = {
        'client_id': client_id,
        'document_type': document_type,
        'action': 'renewal',
        'session_id': session_id
    }
    
    # Agregar document_id si está disponible
    if document_id:
        params['document_id'] = document_id
    
    # Codificar parámetros para URL
    query_string = urllib.parse.urlencode(params)
    
    # 🔧 USAR SOLO ESTA VERSIÓN (días):
    expiry_days = int(os.environ.get('SESSION_EXPIRY_DAYS', '3'))  # 3 días por defecto
    expiry_date = datetime.now() + timedelta(days=expiry_days)
    
    # 🗑️ ELIMINAR ESTAS LÍNEAS DUPLICADAS:
    # expiry_minutes = int(os.environ.get('SESSION_EXPIRY_MINUTES', '1440'))  # 24 horas por defecto
    # expiry_date = datetime.datetime.now() + datetime.timedelta(minutes=expiry_minutes)
 
    # Crear registro de sesión
    session_query = """
    INSERT INTO sesiones (
        id_sesion,
        id_usuario,
        fecha_inicio,
        fecha_expiracion,
        direccion_ip,
        user_agent,
        activa
    ) VALUES (%s, %s, %s, %s, %s, %s, %s)
    """
    
    # Preparar parámetros para la inserción
    session_params = (
        session_id,
        '691d8c44-f524-48fd-b292-be9e31977711',  # 🔧 CAMBIO: Usar client_id real
        datetime.now(),  # 🔧 CAMBIO: Solo datetime.now()
        expiry_date,
        '0.0.0.0',
        '691d8c44-f524-48fd-b292-be9e31977711',
        True,
    )
    
    try:
        # Ejecutar inserción de sesión
        execute_query(session_query, session_params, fetch=False)
        logger.info(f"Sesión creada: {session_id} para cliente {client_id}, válida hasta {expiry_date}")
    except Exception as e:
        logger.error(f"Error creando sesión: {str(e)}")
        # Continuar sin error para que el email se envíe de todas formas

    # Construir URL completa
    renewal_url = f"{BASE_PORTAL_URL}landing/?{query_string}"  # 🔧 Sin / extra
    
    return renewal_url

def flatten_dict(d, parent_key='', sep='_'):
    """
    Transforma un diccionario anidado en uno plano para facilitar sustitución en plantillas
    """
    items = []
    for k, v in d.items():
        new_key = f"{parent_key}{sep}{k}" if parent_key else k
        if isinstance(v, dict):
            items.extend(flatten_dict(v, new_key, sep=sep).items())
        else:
            items.append((new_key, v))
    return dict(items)

def get_client_data_by_id(client_id):
    """
    Obtiene los datos del cliente por su ID
    
    Args:
        client_id: ID del cliente
        
    Returns:
        Dict con los datos del cliente o None si no se encuentra
    """
    try:
        logger.info(f"Buscando cliente con ID: {client_id}")
        
        query = """
        SELECT id_cliente, nombre_razon_social, datos_contacto, segmento_bancario
        FROM gestor_documental.clientes
        WHERE id_cliente = %s
        """
        
        result = execute_query(query, (client_id,))
        logger.info(f"Resultado de la consulta: {result}")
        
        if result and len(result) > 0:
            client_data = result[0]
            logger.info(f"Datos del cliente encontrados: {client_data}")
            
            # Parsear datos_contacto si es un string JSON
            contact_data = client_data.get('datos_contacto')
            if isinstance(contact_data, str):
                try:
                    contact_data = json.loads(contact_data)
                    logger.info(f"Datos de contacto parseados: {contact_data}")
                except json.JSONDecodeError:
                    logger.warning(f"Error parseando datos_contacto para cliente {client_id}")
                    contact_data = {}
            
            client_data['datos_contacto'] = contact_data
            return client_data
        
        logger.warning(f"Cliente no encontrado con ID: {client_id}")
        return None
        
    except Exception as e:
        logger.error(f"Error obteniendo datos del cliente {client_id}: {str(e)}")
        return None

def generate_information_request_email(client_data, request_details=None):
    """
    Genera el contenido HTML del correo de solicitud de información
    
    Args:
        client_data: Datos del cliente
        request_details: Detalles específicos de la solicitud (opcional)
        
    Returns:
        String con contenido HTML del email
    """
    client_name = client_data.get('nombre_razon_social', 'Cliente')
    contact_data = client_data.get('datos_contacto', {})
    client_id = client_data['id_cliente']
    
    # Información por defecto si no se proporcionan detalles específicos
    if not request_details:
        request_details = {
            'tipo_solicitud': 'Actualización de información',
            'documentos_requeridos': [
                'Documento de identidad vigente',
                'Comprobante de domicilio reciente',
                'Estados de cuenta bancarios',
                'Declaración de ingresos'
            ],
            'informacion_requerida': [
                'Datos personales actualizados',
                'Información laboral',
                'Datos de contacto',
                'Información financiera'
            ],
            'plazo_entrega': '15 días hábiles',
            'observaciones': 'Esta información es necesaria para mantener su expediente actualizado y cumplir con las regulaciones vigentes.'
        }
    
    # Generar lista HTML de documentos requeridos
    documentos_html = ""
    for doc in request_details.get('documentos_requeridos', []):
        documentos_html += f"<li>{doc}</li>"
    
    # Generar lista HTML de información requerida
    info_html = ""
    for info in request_details.get('informacion_requerida', []):
        info_html += f"<li>{info}</li>"
    
    # 🆕 Crear sesión específica para solicitud de información
    portal_link = build_information_request_link(client_id, request_details)
    
    email_content = f"""
    <!DOCTYPE html>
    <html lang="es">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>Solicitud de Información</title>
        <style>
            body {{
                font-family: Arial, sans-serif;
                line-height: 1.6;
                color: #333;
                max-width: 600px;
                margin: 0 auto;
                padding: 20px;
            }}
            .header {{
                background-color: #1e3a8a;
                color: white;
                padding: 20px;
                text-align: center;
                border-radius: 8px 8px 0 0;
            }}
            .content {{
                background-color: #f9fafb;
                padding: 30px;
                border-radius: 0 0 8px 8px;
                border: 1px solid #e5e7eb;
            }}
            .section {{
                margin-bottom: 25px;
            }}
            .section h3 {{
                color: #1e3a8a;
                border-bottom: 2px solid #1e3a8a;
                padding-bottom: 5px;
            }}
            ul {{
                padding-left: 20px;
            }}
            li {{
                margin-bottom: 8px;
            }}
            .cta-button {{
                display: inline-block;
                background-color: #1e3a8a;
                color: white;
                padding: 12px 24px;
                text-decoration: none;
                border-radius: 6px;
                margin: 20px 0;
                font-weight: bold;
            }}
            .cta-button:hover {{
                background-color: #1e40af;
            }}
            .footer {{
                margin-top: 30px;
                padding-top: 20px;
                border-top: 1px solid #e5e7eb;
                font-size: 14px;
                color: #6b7280;
            }}
            .important {{
                background-color: #fef3c7;
                border: 1px solid #f59e0b;
                padding: 15px;
                border-radius: 6px;
                margin: 20px 0;
            }}
        </style>
    </head>
    <body>
        <div class="header">
            <h1>🏦 Su Entidad Bancaria</h1>
            <h2>Solicitud de Información</h2>
        </div>
        
        <div class="content">
            <div class="section">
                <p>Estimado/a <strong>{client_name}</strong>,</p>
                
                <p>Le informamos que se ha generado la siguiente solicitud de información para mantener su expediente actualizado:</p>
            </div>
            
            <div class="section">
                <h3>📋 Tipo de Solicitud</h3>
                <p><strong>{request_details.get('tipo_solicitud', 'Actualización de información')}</strong></p>
            </div>
            
            <div class="section">
                <h3>📄 Documentos Requeridos</h3>
                <p>Por favor, proporcione los siguientes documentos:</p>
                <ul>
                    {documentos_html}
                </ul>
            </div>
            
            <div class="section">
                <h3>📝 Información Requerida</h3>
                <p>Complete la siguiente información:</p>
                <ul>
                    {info_html}
                </ul>
            </div>
            
            <div class="important">
                <h3>⏰ Plazo de Entrega</h3>
                <p><strong>Plazo límite: {request_details.get('plazo_entrega', '15 días hábiles')}</strong></p>
                <p>Es importante cumplir con este plazo para evitar interrupciones en nuestros servicios.</p>
            </div>
            
            <div class="section">
                <h3>💻 Acceso al Portal</h3>
                <p>Para facilitar el proceso, puede acceder a nuestro portal digital donde podrá:</p>
                <ul>
                    <li>Completar formularios en línea</li>
                    <li>Subir documentos digitalmente</li>
                    <li>Consultar el estado de su solicitud</li>
                    <li>Recibir notificaciones de avance</li>
                </ul>
                
                <a href="{portal_link}" class="cta-button">Acceder al Portal</a>
            </div>
            
            <div class="section">
                <h3>📞 Soporte</h3>
                <p>Si tiene alguna duda o necesita asistencia, puede contactarnos:</p>
                <ul>
                    <li><strong>Teléfono:</strong> {contact_data.get('telefono', 'N/A')}</li>
                    <li><strong>Email:</strong> {contact_data.get('email', 'N/A')}</li>
                    <li><strong>Dirección:</strong> {contact_data.get('direccion', 'N/A')}</li>
                </ul>
            </div>
            
            <div class="section">
                <h3>📋 Observaciones</h3>
                <p>{request_details.get('observaciones', 'Esta información es necesaria para mantener su expediente actualizado y cumplir con las regulaciones vigentes.')}</p>
            </div>
            
            <div class="footer">
                <p>Este es un mensaje automático del sistema de gestión documental bancario.</p>
                <p>Por favor, no responda a este correo. Para consultas, utilice los canales de contacto mencionados.</p>
            </div>
        </div>
    </body>
    </html>
    """
    
    return email_content

def build_information_request_link(client_id, request_details):
    """
    Construye un link personalizado para solicitud de información con sesión automática
    
    Args:
        client_id: ID del cliente
        request_details: Detalles de la solicitud
        
    Returns:
        String con la URL completa
    """
    
    # Generar token de sesión específico para solicitud de información
    session_id = generate_uuid()

    # Parámetros específicos para solicitud de información
    params = {
        'client_id': client_id,
        'action': 'information_request',
        'session_id': session_id,
        'request_type': request_details.get('tipo_solicitud', 'Actualización de información') if request_details else 'Actualización de información',
        'plazo_entrega': request_details.get('plazo_entrega', '15 días hábiles') if request_details else '15 días hábiles'
    }
    
    # Codificar parámetros para URL
    query_string = urllib.parse.urlencode(params)
    
    # Configurar expiración de sesión (7 días para solicitudes de información)
    expiry_days = int(os.environ.get('INFORMATION_REQUEST_SESSION_DAYS', '7'))  # 7 días por defecto
    expiry_date = datetime.now() + timedelta(days=expiry_days)
    
    # Crear registro de sesión específico para solicitud de información
    session_query = """
    INSERT INTO sesiones (
        id_sesion,
        id_usuario,
        fecha_inicio,
        fecha_expiracion,
        direccion_ip,
        user_agent,
        activa,
        datos_sesion
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """
    
    # Preparar parámetros para la inserción
    session_params = (
        session_id,
        '691d8c44-f524-48fd-b292-be9e31977711',  # Usar client_id como id_usuario
        datetime.now(),
        expiry_date,
        '0.0.0.0',
        'information_request_email',
        True,
        json.dumps({
            'tipo_sesion': 'information_request',
            'request_details': request_details,
            'email_sent_at': datetime.now().isoformat(),
            'portal_action': 'information_request'
        })
    )
    
    try:
        # Ejecutar inserción de sesión
        execute_query(session_query, session_params, fetch=False)
        logger.info(f"Sesión de solicitud de información creada: {session_id} para cliente {client_id}, válida hasta {expiry_date}")
    except Exception as e:
        logger.error(f"Error creando sesión de solicitud de información: {str(e)}")
        # Continuar sin error para que el email se envíe de todas formas

    # Construir URL específica para solicitud de información
    information_request_url = f"{BASE_PORTAL_URL}information-request/?{query_string}"
    
    return information_request_url

def send_information_request_email(client_id, request_details=None):
    """
    Envía un correo de solicitud de información al cliente
    
    Args:
        client_id: ID del cliente
        request_details: Detalles específicos de la solicitud (opcional)
        
    Returns:
        Boolean indicando si el correo se envió correctamente
    """
    try:
        logger.info(f"Iniciando envío de correo para cliente {client_id} con request_details: {request_details}")
        
        # Obtener datos del cliente
        client_data = get_client_data_by_id(client_id)
        if not client_data:
            logger.error(f"No se pudo obtener datos del cliente {client_id}")
            return False
        
        logger.info(f"Datos del cliente obtenidos: {client_data}")
        
        # Obtener email del cliente
        contact_data = client_data.get('datos_contacto', {})
        if not contact_data:
            logger.error(f"Cliente {client_id} no tiene datos de contacto")
            return False
            
        client_email = contact_data.get('email')
        if not client_email:
            logger.error(f"Cliente {client_id} no tiene email registrado")
            return False
        
        logger.info(f"Email del cliente: {client_email}")
        
        # Generar contenido del email
        logger.info("Generando contenido HTML del email...")
        html_content = generate_information_request_email(client_data, request_details)
        logger.info("Contenido HTML generado exitosamente")
        
        # 🆕 Generar URL con sesión para la versión texto plano
        logger.info("Generando URL con sesión...")
        portal_link = build_information_request_link(client_id, request_details)
        logger.info(f"URL generada: {portal_link}")
        
        # Generar versión texto plano
        logger.info("Generando versión texto plano...")
        plain_text = f"""
        Estimado/a {client_data.get('nombre_razon_social', 'Cliente')},
        
        Se ha generado una solicitud de información para mantener su expediente actualizado.
        
        Tipo de Solicitud: {request_details.get('tipo_solicitud', 'Actualización de información') if request_details else 'Actualización de información'}
        
        Documentos Requeridos:
        {chr(10).join([f"- {doc}" for doc in (request_details.get('documentos_requeridos', []) if request_details else ['Documento de identidad vigente', 'Comprobante de domicilio reciente', 'Estados de cuenta bancarios', 'Declaración de ingresos'])])}
        
        Información Requerida:
        {chr(10).join([f"- {info}" for info in (request_details.get('informacion_requerida', []) if request_details else ['Datos personales actualizados', 'Información laboral', 'Datos de contacto', 'Información financiera'])])}
        
        Plazo de Entrega: {request_details.get('plazo_entrega', '15 días hábiles') if request_details else '15 días hábiles'}
        
        Para acceder al portal digital: {portal_link}
        
        Observaciones: {request_details.get('observaciones', 'Esta información es necesaria para mantener su expediente actualizado y cumplir con las regulaciones vigentes.') if request_details else 'Esta información es necesaria para mantener su expediente actualizado y cumplir con las regulaciones vigentes.'}
        
        Saludos cordiales,
        Su Entidad Bancaria
        """
        logger.info("Versión texto plano generada exitosamente")
        
        # Preparar mensaje
        logger.info("Preparando mensaje de email...")
        email_message = {
            'Subject': {
                'Data': 'Solicitud de Información - Su Entidad Bancaria',
                'Charset': 'UTF-8'
            },
            'Body': {
                'Html': {
                    'Data': html_content,
                    'Charset': 'UTF-8'
                },
                'Text': {
                    'Data': plain_text,
                    'Charset': 'UTF-8'
                }
            }
        }
        
        # Enviar correo
        logger.info(f"Enviando correo a {client_email}...")
        ses_response = ses_client.send_email(
            Source=SOURCE_EMAIL,
            Destination={'ToAddresses': [client_email]},
            Message=email_message
        )
        
        logger.info(f"✅ Correo de solicitud de información enviado a {client_email} para cliente {client_id}: {ses_response['MessageId']}")
        
        # Registrar en la base de datos (opcional)
        try:
            notification_data = {
                'id_notificacion': generate_uuid(),
                'id_cliente': client_id,
                'tipo_notificacion': 'solicitud_informacion',
                'destinatario': client_email,
                'asunto': 'Solicitud de Información - Su Entidad Bancaria',
                'contenido': plain_text,
                'fecha_envio': datetime.now(),
                'estado': 'enviado',
                'message_id': ses_response['MessageId'],
                'datos_adicionales': json.dumps({
                    'request_details': request_details,
                    'portal_link': portal_link
                })
            }
            
            # Aquí podrías insertar en una tabla de notificaciones si existe
            # insert_notification_record(notification_data)
            
        except Exception as db_error:
            logger.warning(f"Error registrando notificación en BD: {str(db_error)}")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ Error enviando correo de solicitud de información para cliente {client_id}: {str(e)}")
        import traceback
        logger.error(f"Traceback completo: {traceback.format_exc()}")
        return False