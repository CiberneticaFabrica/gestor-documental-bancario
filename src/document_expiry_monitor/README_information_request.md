# Sistema de Envío de Correos de Solicitud de Información

Este módulo proporciona funcionalidades para enviar correos electrónicos de solicitud de información a clientes del sistema de gestión documental bancario.

## Funciones Principales

### 1. `get_client_data_by_id(client_id)`

Obtiene los datos del cliente desde la base de datos.

**Parámetros:**
- `client_id` (str): ID único del cliente

**Retorna:**
- `dict` o `None`: Datos del cliente incluyendo información de contacto

**Ejemplo:**
```python
from document_expiry_monitor.notification import get_client_data_by_id

client_data = get_client_data_by_id('222a45f8-a4ec-4029-bcda-9c947d836c1a')
if client_data:
    print(f"Cliente: {client_data['nombre_razon_social']}")
    print(f"Email: {client_data['datos_contacto']['email']}")
```

### 2. `send_information_request_email(client_id, request_details=None)`

Envía un correo de solicitud de información al cliente.

**Parámetros:**
- `client_id` (str): ID único del cliente
- `request_details` (dict, opcional): Detalles personalizados de la solicitud

**Retorna:**
- `bool`: `True` si el correo se envió exitosamente, `False` en caso contrario

## Estructura de `request_details`

El parámetro `request_details` es opcional y permite personalizar el contenido del correo:

```python
request_details = {
    'tipo_solicitud': 'Renovación de Documentos KYC',
    'documentos_requeridos': [
        'Cédula de identidad vigente (frente y reverso)',
        'Comprobante de domicilio no mayor a 3 meses',
        'Estados de cuenta de los últimos 3 meses',
        'Declaración jurada de ingresos'
    ],
    'informacion_requerida': [
        'Datos personales actualizados',
        'Información laboral y fuente de ingresos',
        'Datos de contacto actualizados',
        'Información financiera y patrimonio'
    ],
    'plazo_entrega': '10 días hábiles',
    'observaciones': 'Esta renovación es requerida por regulaciones AML/CFT vigentes.'
}
```

## Configuración por Defecto

Si no se proporciona `request_details`, el sistema utiliza la siguiente configuración por defecto:

- **Tipo de solicitud**: "Actualización de información"
- **Documentos requeridos**:
  - Documento de identidad vigente
  - Comprobante de domicilio reciente
  - Estados de cuenta bancarios
  - Declaración de ingresos
- **Información requerida**:
  - Datos personales actualizados
  - Información laboral
  - Datos de contacto
  - Información financiera
- **Plazo de entrega**: 15 días hábiles
- **Observaciones**: Mensaje estándar sobre cumplimiento de regulaciones

## Ejemplos de Uso

### Ejemplo Básico

```python
from document_expiry_monitor.notification import send_information_request_email

# Envío con configuración por defecto
client_id = '222a45f8-a4ec-4029-bcda-9c947d836c1a'
success = send_information_request_email(client_id)

if success:
    print("✅ Correo enviado exitosamente")
else:
    print("❌ Error al enviar el correo")
```

### Ejemplo con Configuración Personalizada

```python
# Configuración personalizada para renovación KYC
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

success = send_information_request_email(client_id, custom_request)
```

### Ejemplo de Envío Masivo

```python
# Lista de clientes para envío masivo
client_ids = [
    '222a45f8-a4ec-4029-bcda-9c947d836c1a',
    '333b56g9-b5fd-5130-cdeb-ad058e947d2b',
    # ... más IDs
]

# Configuración para envío masivo
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

# Envío masivo
success_count = 0
for client_id in client_ids:
    if send_information_request_email(client_id, batch_request):
        success_count += 1

print(f"Correos enviados exitosamente: {success_count}/{len(client_ids)}")
```

## Características del Correo

### Formato HTML
El correo incluye:
- **Encabezado** con logo y título de la entidad bancaria
- **Saludo personalizado** con el nombre del cliente
- **Secciones organizadas** para tipo de solicitud, documentos requeridos, información requerida
- **Destacado visual** para el plazo de entrega
- **Botón de llamada a la acción** para acceder al portal
- **Información de contacto** del cliente
- **Pie de página** con información legal

### Versión Texto Plano
Se incluye una versión en texto plano para compatibilidad con clientes de correo que no soporten HTML.

### Link Personalizado
Cada correo incluye un link único al portal del cliente:
```
https://main.d2ohqmd7t2mj86.amplifyapp.com/client/{client_id}/information-request
```

## Variables de Entorno Requeridas

El sistema requiere las siguientes variables de entorno configuradas:

```bash
# Configuración de AWS SES
SOURCE_EMAIL=notify@softwarefactory.cibernetica.xyz

# Configuración de la base de datos
DB_HOST=your-db-host
DB_NAME=gestor_documental
DB_USER=your-db-user
DB_PASSWORD=your-db-password

# URL del portal
PORTAL_BASE_URL=https://main.d2ohqmd7t2mj86.amplifyapp.com/
```

## Manejo de Errores

El sistema maneja los siguientes errores:

1. **Cliente no encontrado**: Si el `client_id` no existe en la base de datos
2. **Email no disponible**: Si el cliente no tiene email registrado
3. **Error de conexión a BD**: Problemas de conectividad con la base de datos
4. **Error de envío SES**: Problemas con el servicio de email de AWS

Todos los errores se registran en los logs para facilitar el diagnóstico.

## Logs y Monitoreo

El sistema registra:
- ✅ Envíos exitosos con `MessageId` de SES
- ❌ Errores de envío con detalles
- ⚠️ Advertencias sobre datos faltantes
- 📊 Estadísticas de envío masivo

## Pruebas

Para ejecutar las pruebas de ejemplo:

```bash
cd src/document_expiry_monitor
python example_usage.py
```

## Integración con el Sistema

Estas funciones se integran con:
- **Base de datos**: Para obtener datos del cliente
- **AWS SES**: Para envío de correos
- **Portal web**: Para links personalizados
- **Sistema de logs**: Para monitoreo y debugging

## Consideraciones de Seguridad

- Los correos se envían desde una dirección verificada en SES
- Los datos del cliente se obtienen de forma segura desde la base de datos
- Se incluye información de contacto del cliente en el correo
- Los links del portal son personalizados por cliente 