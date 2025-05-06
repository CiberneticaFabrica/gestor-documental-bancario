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
#from notification import generate_notifications

# Configurar logger
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Configuración
NOTIFICATION_DAYS = [30, 15, 5]  # Días de anticipación para notificaciones
SNS_TOPIC_ARN = os.environ.get('NOTIFICATION_TOPIC_ARN')

def lambda_handler(event, context):
    """
    Manejador principal que procesa documentos próximos a vencer
    """
    logger.info("Iniciando monitor de vencimiento de documentos")
    
    try:
        # Obtener fecha actual
        current_date = datetime.now().date()
        
        # Inicializar contadores para métricas
        metrics = {
            'documents_processed': 0,
            'notifications_sent': 0,
            'renewal_requests_created': 0,
            'clients_updated': 0,
            'errors': 0
        }
        
        # Procesar para cada umbral de días configurado
        for days_threshold in NOTIFICATION_DAYS:
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
        
        # Generar resumen de ejecución
        logger.info(f"Resumen de procesamiento: {json.dumps(metrics)}")
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Procesamiento de documentos por vencer completado',
                'metrics': metrics
            })
        }
    
    except Exception as e:
        logger.error(f"Error en el procesamiento de documentos por vencer: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps({
                'message': f'Error en el procesamiento: {str(e)}'
            })
        }