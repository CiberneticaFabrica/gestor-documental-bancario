#!/usr/bin/env python3
"""
Ejemplos de cómo llamar el endpoint de envío de correos de solicitud de información
"""

import requests
import json

# URL base del API (ajustar según tu configuración)
API_BASE_URL = "https://your-api-gateway-url.amazonaws.com/prod"

def send_basic_information_request():
    """
    Ejemplo básico: enviar correo con configuración por defecto
    """
    url = f"{API_BASE_URL}/client/send-information-request"
    
    payload = {
        "client_id": "222a45f8-a4ec-4029-bcda-9c947d836c1a"
    }
    
    headers = {
        "Content-Type": "application/json"
    }
    
    try:
        response = requests.post(url, json=payload, headers=headers)
        
        print(f"Status Code: {response.status_code}")
        print(f"Response: {response.json()}")
        
        if response.status_code == 200:
            print("✅ Correo enviado exitosamente")
        else:
            print("❌ Error al enviar el correo")
            
    except Exception as e:
        print(f"❌ Error en la petición: {str(e)}")

def send_custom_information_request():
    """
    Ejemplo con detalles personalizados de la solicitud
    """
    url = f"{API_BASE_URL}/client/send-information-request"
    
    payload = {
        "client_id": "222a45f8-a4ec-4029-bcda-9c947d836c1a",
        "request_details": {
            "tipo_solicitud": "Renovación de Documentos KYC",
            "documentos_requeridos": [
                "Cédula de identidad vigente (frente y reverso)",
                "Comprobante de domicilio no mayor a 3 meses",
                "Estados de cuenta de los últimos 3 meses",
                "Declaración jurada de ingresos",
                "Certificado de trabajo o constancia de empleo"
            ],
            "informacion_requerida": [
                "Datos personales actualizados",
                "Información laboral y fuente de ingresos",
                "Datos de contacto actualizados",
                "Información financiera y patrimonio",
                "Declaración de beneficiarios finales"
            ],
            "plazo_entrega": "10 días hábiles",
            "observaciones": "Esta renovación es requerida por regulaciones AML/CFT vigentes. El incumplimiento puede resultar en restricciones en sus servicios bancarios."
        }
    }
    
    headers = {
        "Content-Type": "application/json"
    }
    
    try:
        response = requests.post(url, json=payload, headers=headers)
        
        print(f"Status Code: {response.status_code}")
        print(f"Response: {response.json()}")
        
        if response.status_code == 200:
            print("✅ Correo personalizado enviado exitosamente")
        else:
            print("❌ Error al enviar el correo personalizado")
            
    except Exception as e:
        print(f"❌ Error en la petición: {str(e)}")

if __name__ == "__main__":
    print("🏦 API de Envío de Correos de Solicitud de Información")
    print("=" * 60)
    
    # Ejecutar ejemplos
    print("\n1. Envío básico:")
    send_basic_information_request()
    
    print("\n2. Envío con detalles personalizados:")
    send_custom_information_request()
    
    print("\n✨ Ejemplos completados")

# Ejemplo de uso con curl
"""
# Envío básico
curl -X POST https://your-api-gateway-url.amazonaws.com/prod/client/send-information-request \
  -H "Content-Type: application/json" \
  -d '{
    "client_id": "222a45f8-a4ec-4029-bcda-9c947d836c1a"
  }'

# Envío personalizado
curl -X POST https://your-api-gateway-url.amazonaws.com/prod/client/send-information-request \
  -H "Content-Type: application/json" \
  -d '{
    "client_id": "222a45f8-a4ec-4029-bcda-9c947d836c1a",
    "request_details": {
      "tipo_solicitud": "Renovación KYC",
      "documentos_requeridos": [
        "Cédula de identidad vigente",
        "Comprobante de domicilio"
      ],
      "informacion_requerida": [
        "Datos personales actualizados"
      ],
      "plazo_entrega": "10 días hábiles",
      "observaciones": "Renovación requerida por regulaciones vigentes."
    }
  }'
""" 