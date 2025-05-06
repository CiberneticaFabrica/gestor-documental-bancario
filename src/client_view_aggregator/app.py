# src/client_view_aggregator/app.py
import json
import os
import logging
import datetime
import boto3
from common.db_connector import (
    get_client_basic_info,
    calculate_document_completeness,
    determine_document_status,
    calculate_document_risk,
    update_client_view_cache,
    get_all_active_clients
)

# Configurar el logger
logger = logging.getLogger()
logger.setLevel(os.environ.get('LOG_LEVEL', 'INFO'))

def lambda_handler(event, context):
    """
    Función para generar y actualizar la Vista 360° de clientes.
    Calcula métricas de completitud documental y actualiza la cache.
    
    :param event: Evento que puede contener id_cliente específico o ser un evento programado
    :param context: Contexto de Lambda
    :return: Respuesta con estado de procesamiento
    """
    logger.info(f"Evento recibido: {json.dumps(event)}")
    
    # Determinar si es una actualización para un cliente específico o para todos
    if 'id_cliente' in event:
        # Actualizar cliente específico
        id_cliente = event['id_cliente']
        return update_client_view(id_cliente)
    else:
        # Actualizar todos los clientes (evento programado)
        return update_all_client_views()

def update_client_view(id_cliente):
    """
    Actualiza la vista 360° para un cliente específico
    
    :param id_cliente: ID único del cliente
    :return: Resultado de la actualización
    """
    try:
        logger.info(f"Iniciando actualización de vista 360° para cliente {id_cliente}")
        
        # 1. Obtener datos básicos del cliente
        cliente = get_client_basic_info(id_cliente)
        if not cliente:
            logger.error(f"Cliente con ID {id_cliente} no encontrado")
            return {
                'statusCode': 404,
                'body': json.dumps({'message': f'Cliente con ID {id_cliente} no encontrado'})
            }
        
        # 2. Calcular completitud documental
        completitud, docs_pendientes, docs_caducados = calculate_document_completeness(id_cliente)
        logger.info(f"Métricas calculadas para cliente {id_cliente}: Completitud {completitud}%, Pendientes: {docs_pendientes}, Caducados: {docs_caducados}")
        
        # 3. Determinar estado documental
        estado_documental = determine_document_status(completitud, docs_pendientes, docs_caducados)
        
        # 4. Calcular riesgo documental
        riesgo_documental = calculate_document_risk(completitud, docs_caducados, cliente.get('nivel_riesgo', 'bajo'))
        
        # 5. Preparar datos para actualización
        resumen_actividad = {
            'completitud_documental': completitud,
            'documentos_pendientes': docs_pendientes,
            'documentos_caducados': docs_caducados,
            'ultima_actualizacion_documental': datetime.datetime.now().isoformat()
        }
        
        kpis_cliente = {
            'porcentaje_completitud': completitud,
            'estado_documental': estado_documental,
            'riesgo_documental': riesgo_documental
        }
        
        # 6. Actualizar cache del cliente
        update_client_view_cache(id_cliente, resumen_actividad, kpis_cliente)
        
        # 7. Publicar evento de actualización (si se requiere)
        publish_client_update_event(id_cliente, estado_documental, completitud)
        
        logger.info(f"Vista 360° actualizada exitosamente para cliente {id_cliente}")
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': f'Vista 360° actualizada para cliente {id_cliente}',
                'completitud': completitud,
                'docs_pendientes': docs_pendientes,
                'docs_caducados': docs_caducados,
                'estado_documental': estado_documental,
                'riesgo_documental': riesgo_documental
            })
        }
    
    except Exception as e:
        logger.error(f"Error al actualizar vista de cliente {id_cliente}: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        return {
            'statusCode': 500,
            'body': json.dumps({'message': f'Error: {str(e)}'})
        }

def update_all_client_views():
    """
    Actualiza la vista 360° para todos los clientes activos
    
    :return: Resultado de la actualización masiva
    """
    try:
        logger.info("Iniciando actualización masiva de vistas de cliente")
        
        # Obtener todos los clientes activos
        clientes = get_all_active_clients()
        
        # Contadores para estadísticas
        total_clientes = len(clientes)
        actualizados = 0
        fallidos = 0
        
        logger.info(f"Se procesarán {total_clientes} clientes activos")
        
        # Procesar cada cliente
        for cliente in clientes:
            try:
                # Actualizar vista del cliente
                result = update_client_view(cliente['id_cliente'])
                
                # Verificar resultado
                if result['statusCode'] == 200:
                    actualizados += 1
                else:
                    fallidos += 1
                    logger.warning(f"Fallo en cliente {cliente['id_cliente']}: {result['body']}")
            except Exception as e:
                fallidos += 1
                logger.error(f"Error procesando cliente {cliente['id_cliente']}: {str(e)}")
        
        logger.info(f"Actualización masiva completada. Exitosos: {actualizados}, Fallidos: {fallidos}")
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': f'Proceso de actualización completado',
                'clientes_actualizados': actualizados,
                'clientes_fallidos': fallidos,
                'total_clientes': total_clientes
            })
        }
    
    except Exception as e:
        logger.error(f"Error en actualización masiva de clientes: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        return {
            'statusCode': 500,
            'body': json.dumps({'message': f'Error en actualización masiva: {str(e)}'})
        }

def publish_client_update_event(client_id, estado_documental, completitud):
    """
    Publica un evento en EventBridge cuando se actualiza un cliente,
    especialmente útil cuando hay cambios importantes (ej. documento caducado)
    
    :param client_id: ID del cliente
    :param estado_documental: Nuevo estado documental
    :param completitud: Porcentaje de completitud documental
    """
    try:
        # Solo publicar eventos para estados críticos o cambios significativos
        if estado_documental in ['critico', 'incompleto'] or completitud < 80:
            client = boto3.client('events')
            
            # Crear detalle del evento
            detail = {
                'clientId': client_id,
                'documentStatus': estado_documental,
                'completeness': completitud,
                'timestamp': datetime.datetime.now().isoformat()
            }
            
            # Publicar el evento
            response = client.put_events(
                Entries=[
                    {
                        'Source': 'com.bancario.documental',
                        'DetailType': 'ClientDocumentStatusUpdate',
                        'Detail': json.dumps(detail),
                        'EventBusName': 'default'
                    }
                ]
            )
            
            logger.info(f"Evento publicado para cliente {client_id}: {response}")
    except Exception as e:
        # No fallar la función principal si hay problemas con EventBridge
        logger.warning(f"No se pudo publicar evento para cliente {client_id}: {str(e)}")