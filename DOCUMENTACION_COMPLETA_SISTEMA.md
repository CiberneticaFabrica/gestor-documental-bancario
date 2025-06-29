# DOCUMENTACIÓN COMPLETA - GESTOR DOCUMENTAL BANCARIO
## Sistema de Procesamiento Automático Optimizado en AWS

---

## 📋 ÍNDICE
1. [Descripción General](#descripción-general)
2. [Servicios AWS Utilizados](#servicios-aws)
3. [Funciones Lambda](#funciones-lambda)
4. [Arquitectura del Sistema](#arquitectura)
5. [Flujo de Procesamiento](#flujo)
6. [Configuración y Despliegue](#configuración)

---

## 🎯 DESCRIPCIÓN GENERAL

El **Gestor Documental Bancario** es una plataforma serverless completa que automatiza el ciclo de vida de documentos bancarios utilizando IA y servicios AWS.

**Objetivos:**
- Automatización completa de captura y extracción de datos
- Clasificación inteligente de documentos bancarios
- Vista 360° del cliente con evaluación de completitud
- Cumplimiento normativo (KYC, PBC, FATCA)
- Alertas automáticas sobre documentos por vencer

---

## ☁️ SERVICIOS AWS UTILIZADOS

### **1. AWS Lambda** (10 funciones)
- **Runtime**: Python 3.9
- **Configuración Global**: Timeout 30s, Memory 1024MB, Tracing Active
- **Propósito**: Ejecución de lógica de negocio serverless

### **2. Amazon S3** (2 buckets)
- **`{stack-name}-documents-input`**: Documentos de entrada
- **`{stack-name}-documents-processed`**: Documentos procesados
- **Características**: Versioning, CORS, Encriptación, Notificaciones S3→Lambda

### **3. Amazon SQS** (6 colas principales)
- `DocumentClassificationQueue`: Clasificación de documentos
- `IdDocumentProcessorQueue`: Procesamiento de documentos de identidad
- `ContractProcessorQueue`: Procesamiento de contratos
- `FinancialProcessorQueue`: Procesamiento de documentos financieros
- `ThumbnailsQueue`: Generación de miniaturas
- `ModelTrainingQueue`: Entrenamiento de modelos
- **Configuración**: Visibility Timeout 300s, Dead Letter Queues, Max Receive Count 3

### **4. Amazon SNS** (3 tópicos)
- `TextractCompletionTopic`: Notificaciones de finalización de Textract
- `DocumentExpiryNotificationTopic`: Alertas de documentos por vencer
- `DocumentNotificationTopic`: Notificaciones generales

### **5. Amazon Textract**
- **Servicios**: Document Analysis, Document Text Detection, Custom Queries
- **Integración**: SNS para notificaciones, Rol IAM específico

### **6. Amazon Comprehend**
- **Funcionalidades**: DetectEntities, DetectKeyPhrases, ClassifyDocument
- **Uso**: Análisis semántico en TextractCallback

### **7. Amazon RDS MySQL**
- **Host**: `fabrica-gestor-documental.c642nkfthejp.us-east-1.rds.amazonaws.com`
- **Database**: `gestor_documental`
- **Acceso**: Desde VPC privada

### **8. Amazon API Gateway**
- **Endpoints**:
  - `/documents/expiry-monitor` (POST)
  - `/documents/expiry-stats` (GET)
  - `/documents/pending-review` (GET)
  - `/documents/review/{document_id}` (GET/POST)
  - `/documents/review-stats` (GET)
- **CORS**: Habilitado globalmente

### **9. Amazon EventBridge**
- **Reglas**:
  - `ClientViewAggregatorSchedule`: Diario a las 2:00 AM
  - `DocumentExpiryMonitorSchedule`: Diario a las 8:00 AM

### **10. Amazon SES**
- **Propósito**: Envío de notificaciones por email
- **Email origen**: `notify@softwarefactory.cibernetica.xyz`

### **11. AWS SAM**
- **Template**: `template.yaml` (883 líneas)
- **Características**: IaC, Gestión de dependencias, Parámetros configurables

### **12. Amazon VPC**
- **Componentes**: VPC personalizada, 2 subredes privadas, Security Group

### **13. AWS IAM**
- **Roles**: TextractSNSRole, Roles de ejecución Lambda, Políticas específicas

### **14. Amazon CloudWatch**
- **Funcionalidades**: Logs, Métricas, Dashboards, Alertas

---

## 🔧 FUNCIONES LAMBDA DETALLADAS

### **1. DocumentUploadProcessorFunction**
- **Trigger**: Eventos S3 (ObjectCreated)
- **Configuración**: Timeout 300s, Memory 1024MB
- **Funcionalidades**: Validación, inicio Textract, envío a colas, generación miniaturas
- **Permisos**: S3, SQS, Textract, SNS, EventBridge

### **2. TextractCallbackFunction**
- **Trigger**: Notificaciones SNS de Textract
- **Configuración**: Timeout 120s, Memory 1536MB
- **Funcionalidades**: Procesamiento resultados Textract, extracción completa, análisis semántico
- **Permisos**: S3, SQS, Textract, Comprehend, EventBridge

### **3. DocumentClassifierFunction**
- **Trigger**: Mensajes SQS
- **Configuración**: Timeout 30s, Memory 512MB
- **Funcionalidades**: Clasificación inteligente, enrutamiento a procesadores
- **Permisos**: SQS, VPC

### **4. IdDocumentProcessorFunction**
- **Trigger**: Mensajes SQS
- **Configuración**: Timeout 30s, Memory 512MB
- **Funcionalidades**: Procesamiento documentos de identidad
- **Permisos**: VPC

### **5. ContractProcessorFunction**
- **Trigger**: Mensajes SQS
- **Configuración**: Timeout 60s, Memory 512MB
- **Funcionalidades**: Procesamiento contratos bancarios, análisis cláusulas
- **Permisos**: VPC

### **6. FinancialProcessorFunction**
- **Trigger**: Mensajes SQS
- **Configuración**: Timeout 30s, Memory 512MB
- **Funcionalidades**: Procesamiento documentos financieros
- **Permisos**: VPC

### **7. ThumbnailGeneratorFunction**
- **Trigger**: Mensajes SQS
- **Configuración**: Timeout 60s, Memory 1024MB
- **Funcionalidades**: Generación miniaturas, optimización imágenes
- **Permisos**: S3, VPC

### **8. DocumentExpiryMonitorFunction**
- **Trigger**: EventBridge (cron) + API Gateway
- **Configuración**: Timeout 300s, Memory 256MB
- **Funcionalidades**: Monitoreo documentos por vencer, alertas email
- **Permisos**: SNS, SES, S3, EventBridge, VPC

### **9. ClientViewAggregatorFunction**
- **Trigger**: EventBridge (cron diario)
- **Configuración**: Timeout 60s, Memory 256MB
- **Funcionalidades**: Generación vistas 360° clientes, agregaciones
- **Permisos**: VPC, EventBridge

### **10. ManualReviewHandlerFunction**
- **Trigger**: API Gateway
- **Configuración**: Timeout 30s, Memory 512MB
- **Funcionalidades**: Gestión revisión manual documentos
- **Permisos**: S3, SQS, SNS, VPC

---

## 🏗️ ARQUITECTURA DEL SISTEMA

### **Flujo Principal**
```
Usuario → S3 → DocumentUploadProcessor → Textract → TextractCallback
                ↓
            SQS Queues → DocumentClassifier → Procesadores Específicos
                ↓
            Base de Datos ← Actualización de Datos
                ↓
            Monitores y Agregadores → Alertas y Vistas
```

### **Componentes por Capa**

#### **Capa de Entrada**
- S3 Buckets (input/processed)
- API Gateway (endpoints REST)
- EventBridge (eventos programados)

#### **Capa de Procesamiento**
- Lambda Functions (10 funciones especializadas)
- SQS Queues (6 colas de mensajería)
- Textract + Comprehend (IA/ML)

#### **Capa de Datos**
- RDS MySQL (base de datos principal)
- S3 (almacenamiento de documentos)
- CloudWatch (logs y métricas)

#### **Capa de Salida**
- SNS Topics (notificaciones)
- SES (emails)
- API Gateway (APIs de consulta)

---

## 🔄 FLUJO DE PROCESAMIENTO

### **1. Carga de Documentos**
- Usuario sube documento a S3 o mediante API
- Evento S3 activa DocumentUploadProcessor
- Validación inicial del documento

### **2. Procesamiento con IA**
- DocumentUploadProcessor inicia análisis con Textract
- TextractCallback procesa resultados completos
- Extracción de todos los datos posibles de una vez

### **3. Clasificación Inteligente**
- DocumentClassifier analiza datos extraídos
- Clasifica documento según tipo
- Enruta a procesador especializado

### **4. Procesamiento Especializado**
- Procesadores específicos trabajan con datos ya extraídos
- Extraen información específica de su dominio
- Actualizan tablas específicas de la base de datos

### **5. Generación de Miniaturas**
- ThumbnailGenerator crea miniaturas
- Optimiza imágenes para visualización
- Almacena en S3

### **6. Monitoreo y Alertas**
- DocumentExpiryMonitor verifica documentos por vencer
- Envía notificaciones por email
- Genera reportes de cumplimiento

### **7. Agregación de Vistas**
- ClientViewAggregator mantiene vistas 360° actualizadas
- Calcula completitud documental por cliente
- Genera métricas de negocio

---

## ⚙️ CONFIGURACIÓN Y DESPLIEGUE

### **Parámetros de Despliegue**
```yaml
Environment: [dev, staging, prod]
FlowManagerEnabled: ["true", "false"]
DBHost: "fabrica-gestor-documental.c642nkfthejp.us-east-1.rds.amazonaws.com"
DBName: "gestor_documental"
DBUser: "admin"
VpcId: "[REQUIRED]"
PrivateSubnet1: "[REQUIRED]"
PrivateSubnet2: "[REQUIRED]"
TextractRegion: "us-east-1"
PortalBaseUrl: "https://main.d2ohqmd7t2mj86.amplifyapp.com/"
```

### **Comandos de Despliegue**
```bash
# Construir proyecto
sam build

# Desplegar con configuración guiada
sam deploy --guided

# Desplegar con parámetros específicos
sam deploy --parameter-overrides \
  Environment=prod \
  DBPassword=mySecurePassword \
  VpcId=vpc-12345678
```

### **Estructura del Proyecto**
```
gestor-documental-bancario/
├── src/
│   ├── common_layer/           # Capa compartida
│   ├── upload_processor/       # Procesador de carga
│   ├── textract_callback/      # Callback de Textract
│   ├── document_classifier/    # Clasificador
│   ├── processors/             # Procesadores especializados
│   │   ├── id_processor/
│   │   ├── contract_processor/
│   │   └── financial_processor/
│   ├── document_expiry_monitor/ # Monitor de vencimientos
│   ├── client_view_aggregator/  # Agregador de vistas
│   ├── manual_review_handler/   # Gestor de revisiones
│   └── thumbnail_generator/     # Generador de miniaturas
├── template.yaml               # Plantilla SAM
├── tests/                      # Pruebas
└── README.md                   # Documentación
```

### **Configuración Post-Despliegue**
1. Configurar DNS para portal de clientes
2. Configurar SES para envío de emails
3. Configurar CloudWatch dashboards y alertas
4. Configurar CloudTrail para auditoría
5. Configurar GuardDuty para seguridad

---

## 📊 MÉTRICAS Y MONITOREO

### **Métricas Clave**
- **Rendimiento**: Tiempo de ejecución, tasa de éxito, uso de memoria
- **Negocio**: Documentos procesados/hora, precisión de clasificación
- **Infraestructura**: CPU, memoria, latencia, errores de throttling

### **Dashboards CloudWatch**
1. **Operacional**: Métricas de rendimiento en tiempo real
2. **Negocio**: KPIs de procesamiento de documentos
3. **Seguridad**: Alertas y eventos de seguridad
4. **Costos**: Análisis de costos por servicio

### **Alertas Configurables**
- Errores de procesamiento
- Documentos próximos a vencer
- Problemas de rendimiento
- Eventos de seguridad

---

## 🔒 SEGURIDAD

### **VPC y Networking**
- Todas las funciones Lambda en VPC privada
- Security Groups restringidos a RDS (puerto 3306)
- 2 subredes privadas para alta disponibilidad

### **IAM y Permisos**
- Principio de mínimo privilegio
- Roles específicos por función
- Políticas granulares por recurso

### **Encriptación**
- TLS 1.2+ para comunicaciones
- S3 encriptado con SSE-S3
- RDS encriptado con KMS

### **Monitoreo de Seguridad**
- CloudTrail para auditoría
- CloudWatch para monitoreo
- GuardDuty para detección de amenazas

---

## 📞 SOPORTE Y CONTACTO

### **Equipo de Desarrollo**
- **Lead Developer**: Equipo Cibernética Fábrica
- **Email**: notify@softwarefactory.cibernetica.xyz
- **Portal**: https://main.d2ohqmd7t2mj86.amplifyapp.com/

### **Recursos**
- **Documentación Técnica**: Carpeta `docs/`
- **Repositorio**: GitHub con código fuente
- **Issues**: Sistema de tickets para bugs y mejoras

---

*Documento generado automáticamente - Última actualización: Diciembre 2024*
*Versión del Sistema: 2.0 - Arquitectura Optimizada* 