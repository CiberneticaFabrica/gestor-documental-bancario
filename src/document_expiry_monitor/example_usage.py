#!/usr/bin/env python3
"""
Ejemplo de uso de las funciones de envío de correos de solicitud de información
"""

import sys
import os
import json

# Agregar el directorio padre al path para importar módulos
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from document_expiry_monitor.notification import (
    send_information_request_email,
    get_client_data_by_id
)

def example_basic_usage():
    """
    Ejemplo básico de uso con el ID de cliente proporcionado
    """
    # ID del cliente del ejemplo
    client_id = '222a45f8-a4ec-4029-bcda-9c947d836c1a'
    
    print(f"🚀 Enviando correo de solicitud de información para cliente: {client_id}")
    
    # Envío básico con configuración por defecto
    success = send_information_request_email(client_id)
    
    if success:
        print("✅ Correo enviado exitosamente")
    else:
        print("❌ Error al enviar el correo")

def example_custom_request():
    """
    Ejemplo con detalles personalizados de la solicitud
    """
    client_id = '222a45f8-a4ec-4029-bcda-9c947d836c1a'
    
    # Detalles personalizados de la solicitud
    custom_request = {
        'tipo_solicitud': 'Renovación de Documentos KYC',
        'documentos_requeridos': [
            'Cédula de identidad vigente (frente y reverso)',
            'Comprobante de domicilio no mayor a 3 meses',
            'Estados de cuenta de los últimos 3 meses',
            'Declaración jurada de ingresos',
            'Certificado de trabajo o constancia de empleo'
        ],
        'informacion_requerida': [
            'Datos personales actualizados',
            'Información laboral y fuente de ingresos',
            'Datos de contacto actualizados',
            'Información financiera y patrimonio',
            'Declaración de beneficiarios finales'
        ],
        'plazo_entrega': '10 días hábiles',
        'observaciones': 'Esta renovación es requerida por regulaciones AML/CFT vigentes. El incumplimiento puede resultar en restricciones en sus servicios bancarios.'
    }
    
    print(f"🚀 Enviando correo personalizado para cliente: {client_id}")
    
    success = send_information_request_email(client_id, custom_request)
    
    if success:
        print("✅ Correo personalizado enviado exitosamente")
    else:
        print("❌ Error al enviar el correo personalizado")

def example_get_client_data():
    """
    Ejemplo de cómo obtener datos del cliente
    """
    client_id = '222a45f8-a4ec-4029-bcda-9c947d836c1a'
    
    print(f"📋 Obteniendo datos del cliente: {client_id}")
    
    client_data = get_client_data_by_id(client_id)
    
    if client_data:
        print("✅ Datos del cliente obtenidos:")
        print(f"   Nombre: {client_data.get('nombre_razon_social')}")
        print(f"   Email: {client_data.get('datos_contacto', {}).get('email')}")
        print(f"   Teléfono: {client_data.get('datos_contacto', {}).get('telefono')}")
        print(f"   Dirección: {client_data.get('datos_contacto', {}).get('direccion')}")
        print(f"   Segmento: {client_data.get('segmento_bancario')}")
    else:
        print("❌ No se pudieron obtener los datos del cliente")

def example_batch_send():
    """
    Ejemplo de envío masivo a múltiples clientes
    """
    # Lista de IDs de clientes (ejemplo)
    client_ids = [
        '222a45f8-a4ec-4029-bcda-9c947d836c1a',
        # Agregar más IDs según sea necesario
    ]
    
    # Configuración de solicitud para envío masivo
    batch_request = {
        'tipo_solicitud': 'Actualización Anual de Información',
        'documentos_requeridos': [
            'Documento de identidad vigente',
            'Comprobante de domicilio actualizado',
            'Estados de cuenta recientes'
        ],
        'informacion_requerida': [
            'Datos personales actualizados',
            'Información de contacto',
            'Información financiera básica'
        ],
        'plazo_entrega': '20 días hábiles',
        'observaciones': 'Esta actualización anual es parte de nuestro proceso de mantenimiento de expedientes.'
    }
    
    print(f"🚀 Enviando correos masivos a {len(client_ids)} clientes")
    
    success_count = 0
    error_count = 0
    
    for client_id in client_ids:
        try:
            success = send_information_request_email(client_id, batch_request)
            if success:
                success_count += 1
                print(f"✅ Correo enviado a cliente {client_id}")
            else:
                error_count += 1
                print(f"❌ Error enviando correo a cliente {client_id}")
        except Exception as e:
            error_count += 1
            print(f"❌ Excepción enviando correo a cliente {client_id}: {str(e)}")
    
    print(f"\n📊 Resumen del envío masivo:")
    print(f"   ✅ Exitosos: {success_count}")
    print(f"   ❌ Errores: {error_count}")
    print(f"   📧 Total: {len(client_ids)}")

if __name__ == "__main__":
    print("🏦 Sistema de Gestión Documental Bancario")
    print("=" * 50)
    
    # Ejecutar ejemplos
    print("\n1. Ejemplo básico:")
    example_basic_usage()
    
    print("\n2. Ejemplo con datos personalizados:")
    example_custom_request()
    
    print("\n3. Obtener datos del cliente:")
    example_get_client_data()
    
    print("\n4. Envío masivo:")
    example_batch_send()
    
    print("\n✨ Ejemplos completados") 