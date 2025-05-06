# tests/unit/test_document_expiry_monitor.py
import pytest
import json
from datetime import datetime, timedelta
import os
import sys

# Configurar path para importar módulos de la aplicación
sys.path.append('src/document_expiry_monitor')
sys.path.append('src/common_layer/python')

# Mockear imports externos
import boto3
import pymysql
from unittest.mock import patch, MagicMock

# Importar módulos a probar
from app import lambda_handler
from expiry_processor import process_expiring_documents
from notification import send_notification

# Fixtures para pruebas
@pytest.fixture
def sample_documents():
    """
    Proporciona documentos de muestra para pruebas
    """
    today = datetime.now().date()
    return [
        {
            'id_documento': '11111111-1111-1111-1111-111111111111',
            'id_tipo_documento': '22222222-2222-2222-2222-222222222222',
            'titulo': 'DNI de Prueba',
            'id_cliente': '33333333-3333-3333-3333-333333333333',
            'fecha_expiracion': today + timedelta(days=30),
            'tipo_identificacion': 'DNI',
            'numero_identificacion': '12345678X'
        },
        {
            'id_documento': '44444444-4444-4444-4444-444444444444',
            'id_tipo_documento': '55555555-5555-5555-5555-555555555555',
            'titulo': 'Pasaporte de Prueba',
            'id_cliente': '66666666-6666-6666-6666-666666666666',
            'fecha_expiracion': today + timedelta(days=15),
            'tipo_identificacion': 'PASAPORTE',
            'numero_identificacion': 'ABC123456'
        }
    ]

@pytest.fixture
def sample_client():
    """
    Proporciona cliente de muestra para pruebas
    """
    return {
        'id_cliente': '33333333-3333-3333-3333-333333333333',
        'nombre_razon_social': 'Cliente de Prueba',
        'segmento_bancario': 'premium',
        'datos_contacto': {
            'email': 'cliente@ejemplo.com',
            'telefono': '+1234567890'
        },
        'preferencias_comunicacion': {
            'email': True,
            'sms': True
        },
        'gestor_principal_id': '77777777-7777-7777-7777-777777777777'
    }

# Test del manejador Lambda
def test_lambda_handler():
    """
    Prueba el handler Lambda principal
    """
    # Mockear las funciones utilizadas
    with patch('app.get_expiring_documents', return_value=[]), \
         patch('app.process_expiring_documents', return_value={
             'notifications_sent': 0,
             'renewal_requests_created': 0,
             'clients_updated': 0,
             'errors': 0
         }):
             
        # Invocar la función
        event = {}
        context = MagicMock()
        result = lambda_handler(event, context)
        
        # Verificar resultado
        assert result['statusCode'] == 200
        assert 'metrics' in json.loads(result['body'])

# Test del procesador de documentos
def test_process_expiring_documents(sample_documents, sample_client):
    """
    Prueba el procesamiento de documentos por vencer
    """
    # Mockear funciones de DB
    with patch('expiry_processor.get_client_by_id', return_value=sample_client), \
         patch('expiry_processor.update_document_status', return_value=True), \
         patch('expiry_processor.create_document_request', return_value='12345'), \
         patch('expiry_processor.update_client_documental_status', return_value=True), \
         patch('notification.send_notification', return_value=True):
        
        # Procesar documentos de prueba
        results = process_expiring_documents([sample_documents[1]], 15)  # Documento a 15 días
        
        # Verificar resultados
        assert results['notifications_sent'] == 1
        assert results['renewal_requests_created'] == 1
        assert results['clients_updated'] == 1
        assert results['errors'] == 0

# Test de envío de notificaciones
def test_send_notification(sample_client, sample_documents):
    """
    Prueba el envío de notificaciones
    """
    # Mockear SNS y S3
    with patch('notification.sns_client') as mock_sns, \
         patch('notification.s3_client') as mock_s3:
        
        # Configurar mock de SNS publish
        mock_sns.publish.return_value = {'MessageId': '12345'}
        
        # Configurar mock de S3 get_object
        mock_s3.get_object.side_effect = Exception("Template no encontrada")
        
        # Llamar a la función con datos de prueba
        doc = sample_documents[0]
        result = send_notification(sample_client, doc, 30)
        
        # Verificar resultado
        assert result == True
        mock_sns.publish.assert_called_once()