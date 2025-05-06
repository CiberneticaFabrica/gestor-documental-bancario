import logging
from datetime import datetime
import json

from common.db_connector import (
    get_client_by_id,
    update_document_status,
    create_document_request,
    update_client_documental_status
)
from notification import send_notification

# Configurar logger
logger = logging.getLogger()

def process_expiring_documents(expiring_documents, days_threshold):
    """
    Procesa los documentos próximos a vencer, actualiza estados y genera notificaciones
    
    Args:
        expiring_documents: Lista de documentos por vencer
        days_threshold: Días hasta el vencimiento
        
    Returns:
        Dict con métricas del procesamiento
    """
    results = {
        'notifications_sent': 0,
        'renewal_requests_created': 0,
        'clients_updated': 0,
        'errors': 0
    }
    
    # Conjunto para rastrear clientes ya procesados y evitar actualizaciones duplicadas
    processed_clients = set()
    
    for document in expiring_documents:
        try:
            # Obtener información del cliente
            client_id = document['id_cliente']
            client = get_client_by_id(client_id)
            
            if not client:
                logger.warning(f"Cliente no encontrado para documento {document['id_documento']}")
                continue
            
            # Actualizar estado del documento
            update_document_status(
                document['id_documento'], 
                'por_vencer', 
                metadata={
                    'dias_para_vencimiento': days_threshold,
                    'fecha_vencimiento': document['fecha_expiracion'].isoformat()
                }
            )
            
            # Crear solicitud de renovación solo para umbrales específicos
            if days_threshold <= 15:  # Solo crear solicitudes cuando falten 15 días o menos
                request_id = create_document_request(
                    client_id=client_id,
                    document_type_id=document['id_tipo_documento'],
                    expiry_date=document['fecha_expiracion'],
                    notes=f"Renovación automática - Documento vence en {days_threshold} días"
                )
                results['renewal_requests_created'] += 1
                
                # Incluir ID de solicitud en los datos de notificación
                document['id_solicitud'] = request_id
            
            # Generar y enviar notificación
            notification_sent = send_notification(client, document, days_threshold)
            if notification_sent:
                results['notifications_sent'] += 1
            
            # Actualizar estado documental del cliente (solo una vez por cliente)
            if client_id not in processed_clients:
                update_client_documental_status(client_id)
                processed_clients.add(client_id)
                results['clients_updated'] += 1
                
        except Exception as e:
            logger.error(f"Error procesando documento {document['id_documento']}: {str(e)}")
            results['errors'] += 1
    
    return results

def prioritize_documents(documents):
    """
    Prioriza documentos según criterios como tipo, cliente y urgencia
    
    Args:
        documents: Lista de documentos a priorizar
        
    Returns:
        Lista de documentos ordenada por prioridad
    """
    # Prioridades por tipo de documento (valores más bajos = mayor prioridad)
    document_type_priorities = {
        # ID de tipos de documentos de identidad
        '11111111-1111-1111-1111-111111111111': 1,  # DNI/Pasaporte
        '22222222-2222-2222-2222-222222222222': 2,  # Documentos tributarios
        # Añadir más tipos según necesidad
    }
    
    # Prioridades por segmento de cliente (valores más bajos = mayor prioridad)
    client_segment_priorities = {
        'privada': 1,
        'premium': 2,
        'empresas': 3,
        'retail': 4,
        # Añadir más segmentos según necesidad
    }
    
    # Función de puntuación para ordenar
    def priority_score(doc):
        # Menor puntuación = mayor prioridad
        type_priority = document_type_priorities.get(doc['id_tipo_documento'], 999)
        segment_priority = client_segment_priorities.get(doc.get('segmento_bancario', 'otros'), 999)
        days_to_expiry = (doc['fecha_expiracion'] - datetime.now().date()).days
        
        # Fórmula de prioridad combinada
        return (type_priority * 1000) + (segment_priority * 100) + days_to_expiry
    
    # Devolver documentos ordenados por prioridad
    return sorted(documents, key=priority_score)