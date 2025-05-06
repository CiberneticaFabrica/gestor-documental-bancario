# tests/integration/test_document_expiry_integration.py

import pytest
import boto3
import json
import os
import uuid
from datetime import datetime, timedelta
import pymysql

# Configuración de prueba
TEST_DB_HOST = os.environ.get('TEST_DB_HOST', 'localhost')
TEST_DB_NAME = os.environ.get('TEST_DB_NAME', 'gestor_documental_test')
TEST_DB_USER = os.environ.get('TEST_DB_USER', 'test_user')
TEST_DB_PASSWORD = os.environ.get('TEST_DB_PASSWORD', 'test_password')

# Crear conexión a base de datos de prueba
@pytest.fixture
def db_connection():
    conn = pymysql.connect(
        host=TEST_DB_HOST,
        user=TEST_DB_USER,
        password=TEST_DB_PASSWORD,
        database=TEST_DB_NAME,
        charset='utf8mb4',
        cursorclass=pymysql.cursors.DictCursor
    )
    yield conn
    conn.close()

# Configurar datos de prueba
@pytest.fixture
def setup_test_data(db_connection):
    # Crear IDs para los datos de prueba
    client_id = str(uuid.uuid4())
    document_id = str(uuid.uuid4())
    document_type_id = str(uuid.uuid4())
    
    # Fecha actual y de vencimiento (30 días en el futuro)
    today = datetime.now().date()
    expiry_date = today + timedelta(days=30)
    
    conn = db_connection
    try:
        # Crear cliente de prueba
        with conn.cursor() as cursor:
            cursor.execute("""
                INSERT INTO clientes (
                    id_cliente, codigo_cliente, tipo_cliente, 
                    nombre_razon_social, documento_identificacion, 
                    datos_contacto, estado
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s
                )
            """, (
                client_id, 'TEST001', 'persona_fisica',
                'Cliente Prueba Integración', '12345678X',
                json.dumps({
                    'email': 'test@example.com',
                    'telefono': '+123456789'
                }),
                'activo'
            ))
            
            # Crear tipo de documento
            cursor.execute("""
                INSERT INTO tipos_documento (
                    id_tipo_documento, nombre_tipo, descripcion,
                    requiere_aprobacion, es_documento_bancario
                ) VALUES (
                    %s, %s, %s, %s, %s
                )
            """, (
                document_type_id, 'DNI Prueba', 'Documento para pruebas',
                0, 1
            ))
            
            # Crear documento
            cursor.execute("""
                INSERT INTO documentos (
                    id_documento, codigo_documento, id_tipo_documento,
                    titulo, descripcion, version_actual,
                    creado_por, modificado_por, estado
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s
                )
            """, (
                document_id, 'DOC-TEST-001', document_type_id,
                'DNI Prueba Integración', 'Documento para pruebas de integración',
                1, 'test_user', 'test_user', 'publicado'
            ))
            
            # Crear documento de identificación
            cursor.execute("""
                INSERT INTO documentos_identificacion (
                    id_documento, tipo_identificacion, numero_identificacion,
                    pais_emision, fecha_emision, fecha_expiracion, nombre_completo
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s
                )
            """, (
                document_id, 'DNI', '12345678X',
                'España', today, expiry_date, 'Cliente Prueba Integración'
            ))
            
            # Asociar documento a cliente
            cursor.execute("""
                INSERT INTO documentos_clientes (
                    id_documento, id_cliente, fecha_asignacion, asignado_por
                ) VALUES (
                    %s, %s, %s, %s
                )
            """, (
                document_id, client_id, today, 'test_user'
            ))
        
        conn.commit()
        
        # Devolver los IDs para uso en pruebas
        test_data = {
            'client_id': client_id,
            'document_id': document_id,
            'document_type_id': document_type_id,
            'expiry_date': expiry_date
        }
        yield test_data
        
        # Limpieza después de las pruebas
        with conn.cursor() as cursor:
            cursor.execute("DELETE FROM documentos_clientes WHERE id_documento = %s", (document_id,))
            cursor.execute("DELETE FROM documentos_identificacion WHERE id_documento = %s", (document_id,))
            cursor.execute("DELETE FROM documentos WHERE id_documento = %s", (document_id,))
            cursor.execute("DELETE FROM tipos_documento WHERE id_tipo_documento = %s", (document_type_id,))
            cursor.execute("DELETE FROM clientes WHERE id_cliente = %s", (client_id,))
            cursor.execute("DELETE FROM documentos_solicitados WHERE id_cliente = %s", (client_id,))
        
        conn.commit()
        
    except Exception as e:
        conn.rollback()
        raise e

# Prueba de integración con la base de datos
def test_document_expiry_db_integration(db_connection, setup_test_data):
    """
    Prueba la integración del monitor con la base de datos
    """
    from common.db_connector import get_expiring_documents, create_document_request
    
    # Obtener datos de prueba
    test_data = setup_test_data
    
    # Comprobar que se puede obtener el documento por fecha de vencimiento
    expiring_docs = get_expiring_documents(test_data['expiry_date'])
    assert len(expiring_docs) > 0
    assert expiring_docs[0]['id_documento'] == test_data['document_id']
    
    # Probar la creación de una solicitud de renovación
    request_id = create_document_request(
        test_data['client_id'],
        test_data['document_type_id'],
        test_data['expiry_date'],
        "Prueba de integración"
    )
    
    # Verificar que la solicitud se creó correctamente
    conn = db_connection
    with conn.cursor() as cursor:
        cursor.execute(
            "SELECT * FROM documentos_solicitados WHERE id_solicitud = %s",
            (request_id,)
        )
        result = cursor.fetchone()
        
    assert result is not None
    assert result['id_cliente'] == test_data['client_id']
    assert result['id_tipo_documento'] == test_data['document_type_id']
    assert result['estado'] == 'pendiente'