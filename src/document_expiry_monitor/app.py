import json
import os
import logging
from datetime import datetime, timedelta

from common.db_connector import (
    get_expiring_documents,
    get_client_by_id,
    update_document_status,
    create_document_request,
    update_client_documental_status
)
from expiry_processor import process_expiring_documents
from notification import send_information_request_email, get_client_data_by_id

# Configurar logger
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Configuración
NOTIFICATION_DAYS = [30, 15, 5]  # Días de anticipación para notificaciones
SNS_TOPIC_ARN = os.environ.get('NOTIFICATION_TOPIC_ARN')


def lambda_handler(event, context):
    """
    Manejador principal que procesa documentos próximos a vencer
    Maneja tanto ejecuciones programadas como llamadas API
    """
    logger.info(f"Evento recibido: {json.dumps(event)}")
    
    # Determinar el tipo de invocación
    if 'httpMethod' in event:
        # Invocación desde API Gateway
        return handle_api_request(event, context)
    else:
        # Invocación programada (EventBridge/CloudWatch Events)
        return handle_scheduled_execution(event, context)


def handle_api_request(event, context):
    """
    Maneja las peticiones HTTP desde API Gateway
    """
    http_method = event['httpMethod']
    path = event['path']
    
    try:
        # Configurar CORS headers
        cors_headers = {
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Headers': 'Content-Type,X-Amz-Date,Authorization,X-Api-Key,X-Amz-Security-Token',
            'Access-Control-Allow-Methods': 'GET,POST,OPTIONS'
        }
        
        # Manejar preflight requests
        if http_method == 'OPTIONS':
            return {
                'statusCode': 200,
                'headers': cors_headers,
                'body': json.dumps({'message': 'CORS preflight'})
            }
        
        # Parsear el body si existe
        body_data = {}
        if event.get('body'):
            try:
                body_data = json.loads(event['body'])
            except json.JSONDecodeError:
                logger.warning("Body no es JSON válido")
        
        # Enrutar según el path y método
        if path == '/documents/expiry-monitor' and http_method == 'POST':
            return handle_manual_execution(body_data, cors_headers)
        elif path == '/documents/expiry-stats' and http_method == 'GET':
            return handle_expiry_stats(event.get('queryStringParameters', {}), cors_headers)
        elif path == '/client/send-information-request' and http_method == 'POST':
            return handle_send_information_request(body_data, cors_headers)
        elif path == '/client/test' and http_method == 'POST':
            return handle_test_client(body_data, cors_headers)
        else:
            return {
                'statusCode': 404,
                'headers': cors_headers,
                'body': json.dumps({'error': 'Endpoint no encontrado'})
            }
    
    except Exception as e:
        logger.error(f"Error manejando petición API: {str(e)}")
        return {
            'statusCode': 500,
            'headers': cors_headers,
            'body': json.dumps({
                'error': f'Error interno del servidor: {str(e)}'
            })
        }


def handle_manual_execution(body_data, cors_headers):
    """
    Maneja la ejecución manual del monitor de vencimientos
    """
    logger.info("Ejecutando monitor de vencimiento manualmente")
    
    # Permitir configuración personalizada desde el body
    custom_days = body_data.get('notification_days', NOTIFICATION_DAYS)
    force_execution = body_data.get('force', False)
    
    # Validar parámetros
    if not isinstance(custom_days, list) or not all(isinstance(d, int) for d in custom_days):
        return {
            'statusCode': 400,
            'headers': cors_headers,
            'body': json.dumps({
                'error': 'notification_days debe ser una lista de enteros'
            })
        }
    
    try:
        # Ejecutar el procesamiento
        metrics = execute_expiry_monitoring(custom_days, force_execution)
        
        return {
            'statusCode': 200,
            'headers': cors_headers,
            'body': json.dumps({
                'message': 'Ejecución manual completada exitosamente',
                'execution_type': 'manual',
                'timestamp': datetime.now().isoformat(),
                'metrics': metrics,
                'parameters': {
                    'notification_days': custom_days,
                    'force_execution': force_execution
                }
            })
        }
    
    except Exception as e:
        logger.error(f"Error en ejecución manual: {str(e)}")
        return {
            'statusCode': 500,
            'headers': cors_headers,
            'body': json.dumps({
                'error': f'Error ejecutando monitor: {str(e)}'
            })
        }


def handle_expiry_stats(query_params, cors_headers):
    """
    Retorna estadísticas sobre documentos próximos a vencer
    """
    try:
        # Obtener parámetros de consulta
        days_ahead = int(query_params.get('days', 30))
        include_details = query_params.get('details', 'false').lower() == 'true'
        
        current_date = datetime.now().date()
        target_date = current_date + timedelta(days=days_ahead)
        
        # Obtener documentos por vencer
        expiring_documents = get_expiring_documents(target_date)
        
        # Generar estadísticas
        stats = {
            'summary': {
                'total_expiring': len(expiring_documents),
                'query_date': current_date.isoformat(),
                'target_date': target_date.isoformat(),
                'days_ahead': days_ahead
            },
            'by_type': {},
            'by_days_remaining': {}
        }
        
        # Agrupar por tipo de documento
        for doc in expiring_documents:
            doc_type = doc.get('nombre_tipo', 'Desconocido')
            if doc_type not in stats['by_type']:
                stats['by_type'][doc_type] = 0
            stats['by_type'][doc_type] += 1
            
            # Calcular días restantes
            days_remaining = (doc['fecha_expiracion'] - current_date).days
            if days_remaining not in stats['by_days_remaining']:
                stats['by_days_remaining'][days_remaining] = 0
            stats['by_days_remaining'][days_remaining] += 1
        
        # Incluir detalles si se solicita
        if include_details:
            stats['documents'] = [
                {
                    'id': doc['id_documento'],
                    'type': doc.get('nombre_tipo', 'Desconocido'),
                    'client_id': doc['id_cliente'],
                    'expiry_date': doc['fecha_expiracion'].isoformat(),
                    'days_remaining': (doc['fecha_expiracion'] - current_date).days
                }
                for doc in expiring_documents
            ]
        
        return {
            'statusCode': 200,
            'headers': cors_headers,
            'body': json.dumps(stats)
        }
    
    except Exception as e:
        logger.error(f"Error obteniendo estadísticas: {str(e)}")
        return {
            'statusCode': 500,
            'headers': cors_headers,
            'body': json.dumps({
                'error': f'Error obteniendo estadísticas: {str(e)}'
            })
        }


def handle_scheduled_execution(event, context):
    """
    Maneja la ejecución programada del monitor
    """
    logger.info("Ejecutando monitor de vencimiento programado")
    
    try:
        # Ejecutar con configuración por defecto
        metrics = execute_expiry_monitoring(NOTIFICATION_DAYS)
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Procesamiento programado de documentos por vencer completado',
                'execution_type': 'scheduled',
                'timestamp': datetime.now().isoformat(),
                'metrics': metrics
            })
        }
    
    except Exception as e:
        logger.error(f"Error en ejecución programada: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps({
                'message': f'Error en el procesamiento programado: {str(e)}'
            })
        }


def execute_expiry_monitoring(notification_days, force_execution=False):
    """
    Ejecuta el monitoreo de vencimientos con los días especificados
    
    Args:
        notification_days: Lista de días de anticipación
        force_execution: Si True, ejecuta aunque ya se haya ejecutado hoy
        
    Returns:
        Dict con métricas del procesamiento
    """
    current_date = datetime.now().date()
    
    # Inicializar contadores para métricas
    metrics = {
        'documents_processed': 0,
        'notifications_sent': 0,
        'renewal_requests_created': 0,
        'clients_updated': 0,
        'errors': 0,
        'thresholds_processed': []
    }
    
    # Procesar para cada umbral de días configurado
    for days_threshold in notification_days:
        target_date = current_date + timedelta(days=days_threshold)
        logger.info(f"Buscando documentos que vencen en {days_threshold} días ({target_date})")
        
        # Obtener documentos que vencen en ese umbral
        expiring_documents = get_expiring_documents(target_date)
        logger.info(f"Encontrados {len(expiring_documents)} documentos que vencen en {days_threshold} días")
        
        # Procesar los documentos que vencen
        processed_results = process_expiring_documents(expiring_documents, days_threshold)
        
        # Actualizar métricas
        metrics['documents_processed'] += len(expiring_documents)
        metrics['notifications_sent'] += processed_results['notifications_sent']
        metrics['renewal_requests_created'] += processed_results['renewal_requests_created']
        metrics['clients_updated'] += processed_results['clients_updated']
        metrics['errors'] += processed_results['errors']
        
        # Agregar información del umbral procesado
        metrics['thresholds_processed'].append({
            'days_threshold': days_threshold,
            'target_date': target_date.isoformat(),
            'documents_found': len(expiring_documents),
            'results': processed_results
        })
    
    # Generar resumen de ejecución
    logger.info(f"Resumen de procesamiento: {json.dumps(metrics)}")
    
    return metrics


def handle_send_information_request(body_data, cors_headers):
    """
    Maneja el envío de correos de solicitud de información
    """
    logger.info("Procesando solicitud de envío de correo de información")
    
    try:
        # Validar que se proporcione el ID del cliente
        client_id = body_data.get('client_id')
        if not client_id:
            return {
                'statusCode': 400,
                'headers': cors_headers,
                'body': json.dumps({
                    'error': 'Se requiere el parámetro client_id'
                })
            }
        
        # Obtener detalles personalizados si se proporcionan
        request_details = body_data.get('request_details')
        
        # Verificar que el cliente existe antes de enviar
        client_data = get_client_data_by_id(client_id)
        if not client_data:
            return {
                'statusCode': 404,
                'headers': cors_headers,
                'body': json.dumps({
                    'error': f'Cliente con ID {client_id} no encontrado'
                })
            }
        
        # Verificar que el cliente tiene email
        contact_data = client_data.get('datos_contacto', {})
        client_email = contact_data.get('email')
        if not client_email:
            return {
                'statusCode': 400,
                'headers': cors_headers,
                'body': json.dumps({
                    'error': f'El cliente {client_id} no tiene email registrado'
                })
            }
        
        # Enviar el correo
        success = send_information_request_email(client_id, request_details)
        
        if success:
            return {
                'statusCode': 200,
                'headers': cors_headers,
                'body': json.dumps({
                    'message': 'Correo de solicitud de información enviado exitosamente',
                    'client_id': client_id,
                    'client_name': client_data.get('nombre_razon_social'),
                    'client_email': client_email,
                    'timestamp': datetime.now().isoformat(),
                    'request_details': request_details
                })
            }
        else:
            return {
                'statusCode': 500,
                'headers': cors_headers,
                'body': json.dumps({
                    'error': 'Error al enviar el correo de solicitud de información'
                })
            }
    
    except Exception as e:
        logger.error(f"Error enviando correo de solicitud de información: {str(e)}")
        return {
            'statusCode': 500,
            'headers': cors_headers,
            'body': json.dumps({
                'error': f'Error interno del servidor: {str(e)}'
            })
        }


def handle_test_client(body_data, cors_headers):
    """
    Endpoint de prueba para verificar si un cliente existe y sus datos
    """
    logger.info("Procesando solicitud de prueba de cliente")
    
    try:
        # Validar que se proporcione el ID del cliente
        client_id = body_data.get('client_id')
        if not client_id:
            return {
                'statusCode': 400,
                'headers': cors_headers,
                'body': json.dumps({
                    'error': 'Se requiere el parámetro client_id'
                })
            }
        
        # Obtener datos del cliente
        client_data = get_client_data_by_id(client_id)
        
        if client_data:
            return {
                'statusCode': 200,
                'headers': cors_headers,
                'body': json.dumps({
                    'message': 'Cliente encontrado',
                    'client_id': client_id,
                    'client_data': client_data,
                    'has_email': bool(client_data.get('datos_contacto', {}).get('email')),
                    'email': client_data.get('datos_contacto', {}).get('email')
                })
            }
        else:
            return {
                'statusCode': 404,
                'headers': cors_headers,
                'body': json.dumps({
                    'error': f'Cliente con ID {client_id} no encontrado',
                    'client_id': client_id
                })
            }
    
    except Exception as e:
        logger.error(f"Error en prueba de cliente: {str(e)}")
        return {
            'statusCode': 500,
            'headers': cors_headers,
            'body': json.dumps({
                'error': f'Error interno del servidor: {str(e)}'
            })
        }