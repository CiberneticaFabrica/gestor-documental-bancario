import json
import logging
import boto3
import os
from datetime import datetime

# Configurar logger
logger = logging.getLogger()

# Configuración
SNS_TOPIC_ARN = os.environ.get('NOTIFICATION_TOPIC_ARN')
EMAIL_TEMPLATE_BUCKET = os.environ.get('EMAIL_TEMPLATE_BUCKET')
EMAIL_TEMPLATE_KEY_PREFIX = os.environ.get('EMAIL_TEMPLATE_KEY_PREFIX', 'templates/email/')
SOURCE_EMAIL = os.environ.get('SOURCE_EMAIL', 'noreply@ejemplo.com')

# Inicializar clientes AWS
sns_client = boto3.client('sns')
s3_client = boto3.client('s3')
ses_client = boto3.client('ses')

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
                'type': document['id_tipo_documento'],
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
                    'StringValue': document['id_tipo_documento']
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
            "{{document_title}}": notification_data['document']['title'],
            "{{document_expiry_date}}": notification_data['document']['expiry_date'],
            "{{document_days_to_expiry}}": str(notification_data['document']['days_to_expiry']),
            "{{client_segment}}": notification_data['client'].get('segment', 'General')
        }
        
        # Aplicar reemplazos
        email_content = template_content
        for placeholder, value in replacements.items():
            email_content = email_content.replace(placeholder, str(value))
            
    except Exception as e:
        # Fallback a plantilla básica si hay error
        logger.warning(f"Error cargando plantilla de email: {str(e)}")
        
        client_name = notification_data['client']['name']
        doc_type = notification_data['document']['title']
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