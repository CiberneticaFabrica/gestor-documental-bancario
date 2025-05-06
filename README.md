Gestor Documental Bancario - Sistema de Procesamiento Automático
Sistema serverless para la automatización de la captura, clasificación, procesamiento y gestión de documentos bancarios utilizando inteligencia artificial en AWS.
Descripción General
El Sistema de Gestión Documental Bancario es una plataforma serverless basada en AWS que automatiza el ciclo de vida completo de documentos bancarios. El sistema proporciona una Vista 360° de clientes, alerta sobre documentos próximos a vencer, y facilita el cumplimiento normativo del sector bancario.
Objetivos principales

Automatizar la captura y extracción de datos de documentos bancarios
Proporcionar clasificación inteligente de documentos
Facilitar la gestión del ciclo de vida documental completo
Ofrecer una Vista 360° del cliente con evaluación de completitud documental
Garantizar cumplimiento normativo (KYC, PBC, FATCA)
Alertar sobre documentos próximos a vencer
Integrarse con sistemas bancarios existentes

Arquitectura
El sistema utiliza una arquitectura serverless optimizada, enfocada en maximizar la eficiencia y reducir costos:
Mostrar imagen
Componentes principales

DocumentUploadProcessor: Procesa documentos cargados en S3 e inicia el procesamiento
TextractCallback: Centro de procesamiento que extrae todos los datos posibles de los documentos
DocumentClassifier: Enrutador inteligente que clasifica y dirige documentos a procesadores específicos
Procesadores especializados:

IdDocumentProcessor: Procesa documentos de identidad (DNI, pasaportes)
ContractProcessor: Procesa contratos bancarios
FinancialProcessor: Procesa extractos, nóminas y documentos financieros


Monitores y agregadores:

DocumentExpiryMonitor: Alerta sobre documentos próximos a vencer
ClientViewAggregator: Genera vistas 360° de clientes



Flujo de Procesamiento Optimizado
El sistema implementa un flujo de procesamiento optimizado que evita el procesamiento redundante:

Carga de documento:

Usuario sube documento a S3 o mediante interfaz web
Evento S3 activa DocumentUploadProcessor


Procesamiento central:

DocumentUploadProcessor inicia procesamiento con Textract
TextractCallback procesa los resultados y extrae TODOS los datos posibles de una sola vez
Se almacena información en base de datos


Clasificación inteligente:

DocumentClassifier clasifica los documentos utilizando datos ya extraídos
Dirige el documento al procesador especializado correspondiente


Procesamiento especializado:

Los procesadores especializados solo trabajan con datos ya extraídos
Extraen información específica de su dominio
Actualizan tablas específicas de la base de datos


Monitorización y actualización:

DocumentExpiryMonitor verifica documentos próximos a caducar
ClientViewAggregator mantiene actualizada la vista de cliente



Ventajas del Diseño Optimizado

Reducción de costos: Minimiza las llamadas a servicios de IA/ML
Mayor rendimiento: Elimina el procesamiento redundante
Centralización del procesamiento pesado: Un único punto de extracción de datos
Tolerancia a fallos: Sistema de reintentos y recuperación
Escalabilidad: Componentes desacoplados que escalan independientemente

Estructura del Proyecto
gestor-documental-bancario/
├── .aws-sam/                  # Directorio de construcción de AWS SAM
├── src/                       # Código fuente principal
│   ├── common_layer/         # Capa compartida para todas las funciones Lambda
│   │   └── python/
│   │       └── common/
│   │           ├── validation.py     # Funciones de validación comunes
│   │           ├── db_connector.py   # Conector a base de datos RDS
│   │           └── s3_utils.py       # Utilidades para S3
│   │
│   ├── upload_processor/     # Procesador de carga inicial
│   │   ├── app.py            # Lambda handler para procesamiento inicial
│   │   └── document_validator.py  # Validación de documentos
│   │
│   ├── textract_callback/    # Procesador de resultados de Textract
│   │   └── app.py            # Extrae todos los datos posibles
│   │
│   ├── document_classifier/  # Clasificador de documentos optimizado
│   │   ├── app.py            # Lambda handler para clasificación
│   │   └── classifier.py     # Lógica de clasificación
│   │
│   ├── processors/           # Procesadores especializados optimizados
│   │   ├── financial_processor/
│   │   │   ├── app.py  
│   │   │   └── financial_parser.py
│   │   ├── contract_processor/
│   │   │   ├── app.py
│   │   │   └── contract_parser.py
│   │   └── id_processor/
│   │       └── app.py
│   │
│   ├── document_expiry_monitor/
│   │   └── app.py            # Monitor de documentos por vencer
│   │
│   └── client_view_aggregator/
│       └── app.py            # Generador de vista 360° de cliente
│
├── template.yaml             # Plantilla SAM para infraestructura
├── tests/                    # Pruebas
└── README.md                 # Este archivo
Configuración y Despliegue
Requisitos previos

AWS CLI configurada con credenciales apropiadas
AWS SAM CLI instalada
Python 3.9+ instalado

Configuración de la Base de Datos
Antes de desplegar la aplicación, debe crear la base de datos MySQL con el esquema proporcionado:
bashmysql -h <DB_HOST> -u <DB_USER> -p < esquemaDatabaseGestor.sql
Despliegue con SAM

Construir el proyecto:

bashsam build

Desplegar en AWS:

bashsam deploy --guided

Parámetros a configurar:


DBHost: Host de la base de datos MySQL
DBName: Nombre de la base de datos
DBUser: Usuario de la base de datos
DBPassword: Contraseña de la base de datos
VpcId: ID de la VPC donde se desplegarán las funciones
PrivateSubnet1: Primera subred privada
PrivateSubnet2: Segunda subred privada

Uso del Sistema
Carga de Documentos
Los documentos pueden cargarse de dos formas:

Directamente a S3:

Subir documentos a la carpeta incoming/ del bucket S3 <stack-name>-documents-input


Mediante API Gateway (si está configurada):

Usar las rutas de API para cargar documentos
Autenticar con API Key o Cognito según configuración



Monitoreo y Logging

Los logs están disponibles en CloudWatch Logs
Cada función Lambda tiene su propio grupo de logs
Se recomienda configurar dashboards en CloudWatch para visualizar:

Tiempo de procesamiento por tipo de documento
Tasa de éxito de clasificación
Documentos próximos a vencer
Completitud documental por cliente



Notas de Desarrollo

La arquitectura optimizada centraliza el procesamiento pesado en TextractCallback
Los procesadores específicos solo trabajan con datos ya extraídos
Todas las Lambdas tienen acceso a VPC para conectar con la base de datos
El procesamiento asíncrono es gestionado mediante colas SQS
Las notificaciones son gestionadas mediante tópicos SNS

Seguridad

Todo el tráfico entre componentes está cifrado en tránsito
Los datos en S3 están cifrados en reposo
Las credenciales de base de datos se gestionan mediante variables de entorno
El acceso a las funciones Lambda está regulado por políticas IAM
El acceso a la base de datos está restringido al security group de Lambda

Extensibilidad
El sistema es altamente extensible y permite agregar:

Nuevos tipos de documentos
Nuevos procesadores especializados
Nuevas integraciones con sistemas bancarios
Nuevos flujos de trabajo para casos de uso específicos


Para obtener más información, consultar la documentación detallada en la carpeta docs/ o contactar al equipo de desarrollo.