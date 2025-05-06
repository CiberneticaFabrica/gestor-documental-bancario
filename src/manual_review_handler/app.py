import json
import logging
import os
import datetime
import boto3
import uuid
from decimal import Decimal

# Import common layer utilities
from common.db_connector import (
    get_pending_review_documents,
    check_document_access,
    get_document_review_data,
    submit_document_review,
    get_review_statistics,
    insert_audit_record,
    get_connection
)
from common.s3_utils import generate_s3_presigned_url

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Initialize AWS services
s3_client = boto3.client('s3')
sqs_client = boto3.client('sqs')
sns_client = boto3.client('sns')

# Environment variables
DOCUMENTS_BUCKET = os.environ['DOCUMENTS_BUCKET']
MODEL_TRAINING_QUEUE_URL = os.environ.get('MODEL_TRAINING_QUEUE_URL', '')
NOTIFICATION_TOPIC_ARN = os.environ.get('NOTIFICATION_TOPIC_ARN', '')

def lambda_handler(event, context):
    """
    Handler for ManualReviewHandler - procesa las solicitudes de API Gateway para operaciones de revisión manual
    """
    logger.info(f"Received event: {json.dumps(event)}")
    
    try:
        # Extract HTTP method and path
        http_method = event['httpMethod']
        path = event['path']
        
        # Authenticate user
        auth_result = authenticate_user(event)
        if not auth_result['authenticated']:
            return {
                'statusCode': 401,
                'headers': {'Content-Type': 'application/json'},
                'body': json.dumps({'error': auth_result['message']})
            }
            
        # Add user info to the event
        event['user'] = auth_result['user']
        
        # Check if user has required permissions
        if not check_user_permissions(event['user'], path, http_method):
            return {
                'statusCode': 403,
                'headers': {'Content-Type': 'application/json'},
                'body': json.dumps({'error': 'No tienes permisos suficientes para realizar esta operación'})
            }
        
        # Route based on HTTP method and path
        if http_method == 'GET' and '/documents/pending-review' in path:
            return get_pending_reviews(event)
        elif http_method == 'GET' and '/documents/review/' in path:
            # Extract document_id from path
            document_id = path.split('/documents/review/')[1]
            return get_document_for_review(event, document_id)
        elif http_method == 'POST' and '/documents/review/' in path:
            # Extract document_id from path
            document_id = path.split('/documents/review/')[1]
            return submit_review(event, document_id)
        elif http_method == 'GET' and '/documents/review-stats' in path:
            return get_review_statistics_endpoint(event)
        else:
            return {
                'statusCode': 404,
                'headers': {'Content-Type': 'application/json'},
                'body': json.dumps({'error': 'Ruta no encontrada'})
            }
            
    except Exception as e:
        logger.error(f"Error processing request: {str(e)}")
        return {
            'statusCode': 500,
            'headers': {'Content-Type': 'application/json'},
            'body': json.dumps({'error': f'Error interno del servidor: {str(e)}'})
        }

def authenticate_user(event):
    """
    Autentica al usuario basado en el token de sesión
    """
    # Extract Authorization header
    auth_header = event.get('headers', {}).get('Authorization', '')
    
    if not auth_header or not auth_header.startswith('Bearer '):
        return {
            'authenticated': False,
            'message': 'Token de autenticación no proporcionado o inválido'
        }
    
    session_token = auth_header.split(' ')[1]
    
    try:
        connection = get_connection()
        try:
            with connection.cursor() as cursor:
                # Check if session exists and is valid
                session_query = """
                    SELECT s.id_sesion, s.id_usuario, s.fecha_expiracion, s.activa,
                           u.id_usuario, u.nombre_usuario, u.nombre, u.apellidos, u.email, u.estado
                    FROM sesiones s
                    JOIN usuarios u ON s.id_usuario = u.id_usuario
                    WHERE s.id_sesion = %s
                      AND s.fecha_expiracion > NOW()
                      AND s.activa = 1
                      AND u.estado = 'activo'
                """
                
                cursor.execute(session_query, [session_token])
                session_result = cursor.fetchall()
                
                if not session_result:
                    return {
                        'authenticated': False,
                        'message': 'Sesión inválida o expirada'
                    }
                
                user = session_result[0]
                
                # Get user roles and permissions
                roles_query = """
                    SELECT r.id_rol, r.nombre_rol, ur.ambito, ur.id_ambito
                    FROM usuarios_roles ur
                    JOIN roles r ON ur.id_rol = r.id_rol
                    WHERE ur.id_usuario = %s
                """
                
                cursor.execute(roles_query, [user['id_usuario']])
                roles = cursor.fetchall()
                
                perms_query = """
                    SELECT p.id_permiso, p.codigo_permiso, p.categoria
                    FROM roles_permisos rp
                    JOIN permisos p ON rp.id_permiso = p.id_permiso
                    WHERE rp.id_rol IN (SELECT id_rol FROM usuarios_roles WHERE id_usuario = %s)
                """
                
                cursor.execute(perms_query, [user['id_usuario']])
                permissions = cursor.fetchall()
                
                user['roles'] = roles
                user['permissions'] = permissions
                
                # Update last access
                update_query = """
                    UPDATE usuarios
                    SET ultimo_acceso = NOW()
                    WHERE id_usuario = %s
                """
                
                cursor.execute(update_query, [user['id_usuario']])
                connection.commit()
                
                # Register audit log
                ip_address = event.get('requestContext', {}).get('identity', {}).get('sourceIp', '0.0.0.0')
                user_agent = event.get('headers', {}).get('User-Agent', '')
                
                # Preparar datos para registro de auditoría
                audit_data = {
                    'fecha_hora': datetime.datetime.now(),
                    'usuario_id': user['id_usuario'],
                    'direccion_ip': ip_address,
                    'accion': 'ver',
                    'entidad_afectada': 'documento',
                    'id_entidad_afectada': None,
                    'detalles': json.dumps({
                        'path': event['path'],
                        'method': event['httpMethod'],
                        'user_agent': user_agent
                    }),
                    'resultado': 'exito'
                }
                
                # Registrar en auditoría
                insert_audit_record(audit_data)
                
                return {
                    'authenticated': True,
                    'user': user
                }
        finally:
            connection.close()
    except Exception as e:
        logger.error(f"Error authenticating user: {str(e)}")
        return {
            'authenticated': False,
            'message': f'Error al autenticar: {str(e)}'
        }
            
def check_user_permissions(user, path, method):
    """
    Verifica si el usuario tiene los permisos requeridos para la acción solicitada
    """
    # Extract permissions from user object
    permissions = user.get('permissions', [])
    permission_codes = [p['codigo_permiso'] for p in permissions]
    
    # Define required permissions for each endpoint
    required_perms = {
        'GET:/documents/pending-review': 'documento.listar_revision',
        'GET:/documents/review/': 'documento.ver_revision',
        'POST:/documents/review/': 'documento.validar_revision',
        'GET:/documents/review-stats': 'documento.estadisticas_revision'
    }
    
    # Check if user has admin permission (override)
    if 'admin.todas_operaciones' in permission_codes:
        return True
    
    # Find matching path
    for endpoint, perm in required_perms.items():
        if endpoint in f"{method}:{path}" or (endpoint.endswith('/') and f"{method}:{path.split('/')[0]}" == endpoint):
            return perm in permission_codes
    
    # Default: deny access
    return False

def get_pending_reviews(event):
    """
    Devuelve una lista de documentos pendientes de revisión manual, con soporte para paginación
    """
    try:
        # Parse query parameters
        query_params = event.get('queryStringParameters', {}) or {}
        page = int(query_params.get('page', 1))
        page_size = int(query_params.get('page_size', 10))
        tipo_documento = query_params.get('tipo_documento', None)
        nivel_confianza = query_params.get('nivel_confianza', None)
        
        # Get user information
        user = event['user']
        
        # Check if user is admin
        is_admin = 'admin.todas_operaciones' in [p['codigo_permiso'] for p in user.get('permissions', [])]
        
        # Call db function to get documents
        documents, pagination = get_pending_review_documents(
            tipo_documento=tipo_documento, 
            nivel_confianza=nivel_confianza, 
            user_id=user['id_usuario'], 
            is_admin=is_admin, 
            page=page, 
            page_size=page_size
        )
        
        # Process results to include presigned URLs for documents
        processed_docs = []
        for doc in documents:
            # Generate presigned URL for viewing the document
            doc_key = doc['ubicacion_almacenamiento_ruta']
            doc['view_url'] = generate_s3_presigned_url(
                bucket=DOCUMENTS_BUCKET,
                key=doc_key,
                expiration=3600  # URL valid for 1 hour
            )
            
            # Convert any Decimal objects to float for JSON serialization
            doc = {k: float(v) if isinstance(v, Decimal) else v for k, v in doc.items()}
            processed_docs.append(doc)
        
        # Registrar actividad de listar documentos pendientes
        ip_address = event.get('requestContext', {}).get('identity', {}).get('sourceIp', '0.0.0.0')
        
        # Preparar datos para registro de auditoría
        audit_data = {
            'fecha_hora': datetime.datetime.now(),
            'usuario_id': user['id_usuario'],
            'direccion_ip': ip_address,
            'accion': 'ver',
            'entidad_afectada': 'documento',
            'id_entidad_afectada': None,
            'detalles': json.dumps({
                'action': 'list_pending_reviews',
                'filters': {
                    'tipo_documento': tipo_documento,
                    'nivel_confianza': nivel_confianza
                },
                'pagination': {
                    'page': page,
                    'page_size': page_size
                },
                'results_count': len(processed_docs)
            }),
            'resultado': 'exito'
        }
        
        # Registrar en auditoría
        insert_audit_record(audit_data)
        
        return {
            'statusCode': 200,
            'headers': {'Content-Type': 'application/json'},
            'body': json.dumps({
                'documents': processed_docs,
                'pagination': pagination
            })
        }
    except Exception as e:
        logger.error(f"Error fetching pending reviews: {str(e)}")
        return {
            'statusCode': 500,
            'headers': {'Content-Type': 'application/json'},
            'body': json.dumps({'error': f'Error al obtener documentos pendientes: {str(e)}'})
        }

def get_document_for_review(event, document_id):
    """
    Devuelve información detallada sobre un documento para revisión manual
    """
    try:
        # Get user information
        user = event['user']
        
        # Check if user has access to this document
        if not check_document_access(document_id, user['id_usuario']):
            return {
                'statusCode': 403,
                'headers': {'Content-Type': 'application/json'},
                'body': json.dumps({'error': 'No tienes permisos para acceder a este documento'})
            }
        
        # Get document data
        document_data = get_document_review_data(document_id)
        
        if not document_data:
            return {
                'statusCode': 404,
                'headers': {'Content-Type': 'application/json'},
                'body': json.dumps({'error': 'Documento no encontrado o no requiere revisión'})
            }
        
        # Generate presigned URL for viewing the document
        doc_key = document_data['document']['ubicacion_almacenamiento_ruta']
        view_url = generate_s3_presigned_url(
            bucket=DOCUMENTS_BUCKET,
            key=doc_key,
            expiration=3600  # URL valid for 1 hour
        )
        
        # Add view URL to response
        document_data['view_url'] = view_url
        
        # Prepare data for JSON response
        # Handle JSON fields if they are strings
        for field in ['entidades_detectadas', 'metadatos_extraccion']:
            if field in document_data['document'] and document_data['document'][field]:
                if isinstance(document_data['document'][field], str):
                    try:
                        document_data['document'][field] = json.loads(document_data['document'][field])
                    except:
                        pass
        
        # Convert Decimal to float for JSON serialization
        def decimal_to_float(obj):
            if isinstance(obj, dict):
                return {k: decimal_to_float(v) for k, v in obj.items()}
            elif isinstance(obj, list):
                return [decimal_to_float(x) for x in obj]
            elif isinstance(obj, Decimal):
                return float(obj)
            else:
                return obj
        
        document_data = decimal_to_float(document_data)
        
        # Registrar actividad de ver documento
        ip_address = event.get('requestContext', {}).get('identity', {}).get('sourceIp', '0.0.0.0')
        
        # Preparar datos para registro de auditoría
        audit_data = {
            'fecha_hora': datetime.datetime.now(),
            'usuario_id': user['id_usuario'],
            'direccion_ip': ip_address,
            'accion': 'ver',
            'entidad_afectada': 'documento',
            'id_entidad_afectada': document_id,
            'detalles': json.dumps({
                'action': 'get_document_for_review',
                'document_code': document_data['document']['codigo_documento']
            }),
            'resultado': 'exito'
        }
        
        # Registrar en auditoría
        insert_audit_record(audit_data)
        
        return {
            'statusCode': 200,
            'headers': {'Content-Type': 'application/json'},
            'body': json.dumps(document_data)
        }
    except Exception as e:
        logger.error(f"Error fetching document for review: {str(e)}")
        return {
            'statusCode': 500,
            'headers': {'Content-Type': 'application/json'},
            'body': json.dumps({'error': f'Error al obtener documento para revisión: {str(e)}'})
        }

def submit_review(event, document_id):
    """
    Procesa el envío de una revisión y actualiza los datos del documento
    """
    try:
        # Get user information
        user = event['user']
        
        # Parse request body
        body = json.loads(event['body'])
        
        # Extract review data
        analysis_id = body.get('analysis_id')
        verification_status = body.get('verification_status')
        verification_notes = body.get('verification_notes', '')
        corrected_data = body.get('corrected_data', {})
        document_type_confirmed = body.get('document_type_confirmed')
        
        # Validate required fields
        if not all([analysis_id, verification_status]):
            return {
                'statusCode': 400,
                'headers': {'Content-Type': 'application/json'},
                'body': json.dumps({'error': 'Faltan campos requeridos'})
            }
        
        # Check if user has access to modify this document
        if not check_document_access(document_id, user['id_usuario'], require_write=True):
            return {
                'statusCode': 403,
                'headers': {'Content-Type': 'application/json'},
                'body': json.dumps({'error': 'No tienes permisos para modificar este documento'})
            }
        
        # Submit review to database
        result = submit_document_review(
            document_id=document_id,
            analysis_id=analysis_id,
            user_id=user['id_usuario'],
            verification_status=verification_status,
            verification_notes=verification_notes,
            corrected_data=corrected_data,
            document_type_confirmed=document_type_confirmed
        )
        
        # If successful, send to model training queue
        if result and MODEL_TRAINING_QUEUE_URL:
            # Prepare training message
            training_message = {
                'event_type': 'document_verification',
                'document_id': document_id,
                'analysis_id': analysis_id,
                'verification_status': verification_status,
                'corrected_data': corrected_data,
                'timestamp': datetime.datetime.now().isoformat()
            }
            
            # Send to queue
            sqs_client.send_message(
                QueueUrl=MODEL_TRAINING_QUEUE_URL,
                MessageBody=json.dumps(training_message)
            )
        
        # If document was rejected and notifications are enabled, send notification
        if verification_status == 'rejected' and NOTIFICATION_TOPIC_ARN:
            notification_message = {
                'event_type': 'document_rejected',
                'document_id': document_id,
                'document_code': body.get('document_code'),
                'verified_by': user['id_usuario'],
                'verification_notes': verification_notes,
                'timestamp': datetime.datetime.now().isoformat()
            }
            
            sns_client.publish(
                TopicArn=NOTIFICATION_TOPIC_ARN,
                Message=json.dumps(notification_message),
                Subject=f"Documento Rechazado: {body.get('document_code', document_id)}"
            )
        
        return {
            'statusCode': 200,
            'headers': {'Content-Type': 'application/json'},
            'body': json.dumps({
                'message': 'Revisión enviada exitosamente',
                'document_id': document_id,
                'analysis_id': analysis_id
            })
        }
    except Exception as e:
        logger.error(f"Error submitting review: {str(e)}")
        return {
            'statusCode': 500,
            'headers': {'Content-Type': 'application/json'},
            'body': json.dumps({'error': f'Error al enviar revisión: {str(e)}'})
        }

def get_review_statistics_endpoint(event):
    """
    Devuelve estadísticas sobre revisiones manuales
    """
    try:
        # Get user information
        user = event['user']
        
        # Get statistics from db
        stats = get_review_statistics()
        
        # Convert any Decimal to float for JSON serialization
        def decimal_to_float(obj):
            if isinstance(obj, dict):
                return {k: decimal_to_float(v) for k, v in obj.items()}
            elif isinstance(obj, list):
                return [decimal_to_float(x) for x in obj]
            elif isinstance(obj, Decimal):
                return float(obj)
            else:
                return obj
        
        stats = decimal_to_float(stats)
        
        # Handle dates in trend_stats (convert to string)
        for item in stats.get('trend_stats', []):
            if 'review_date' in item and isinstance(item['review_date'], datetime.date):
                item['review_date'] = item['review_date'].isoformat()
        
        # Registrar actividad
        ip_address = event.get('requestContext', {}).get('identity', {}).get('sourceIp', '0.0.0.0')
        
        # Preparar datos para registro de auditoría
        audit_data = {
            'fecha_hora': datetime.datetime.now(),
            'usuario_id': user['id_usuario'],
            'direccion_ip': ip_address,
            'accion': 'ver',
            'entidad_afectada': 'documento',
            'id_entidad_afectada': None,
            'detalles': json.dumps({
                'action': 'get_review_statistics'
            }),
            'resultado': 'exito'
        }
        
        # Registrar en auditoría
        insert_audit_record(audit_data)
        
        return {
            'statusCode': 200,
            'headers': {'Content-Type': 'application/json'},
            'body': json.dumps(stats)
        }
    except Exception as e:
        logger.error(f"Error fetching review statistics: {str(e)}")
        return {
            'statusCode': 500,
            'headers': {'Content-Type': 'application/json'},
            'body': json.dumps({'error': f'Error al obtener estadísticas de revisión: {str(e)}'})
        }