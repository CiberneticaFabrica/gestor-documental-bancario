import os
from datetime import datetime
import logging
 
logger = logging.getLogger()
logger.setLevel(os.environ.get('LOG_LEVEL', 'INFO'))

def get_queries_for_document_type(doc_type):
    """
    Retorna queries específicas según el tipo de documento
    Basado en el análisis del contrato de préstamo personal
    """
    
    if doc_type in ['contrato', 'contrato_prestamo', 'prestamo_personal']:
        return get_loan_contract_queries()
    elif doc_type in ['contrato_cuenta', 'apertura_cuenta']:
        return get_account_contract_queries()
    elif doc_type in ['contrato_tarjeta', 'tarjeta_credito']:
        return get_credit_card_queries()
    elif doc_type in ['dni', 'cedula', 'pasaporte']:
        return get_id_document_queries()
    else:
        return get_generic_contract_queries()

def get_loan_contract_queries():
    """
    VERSIÓN MEJORADA: Queries validadas para contratos de préstamo
    """
    queries = [
        {
            'Text': '¿Cuál es el número del contrato?',
            'Alias': 'numero_contrato'
        },
        {
            'Text': '¿Cuál es el nombre completo del prestatario?',
            'Alias': 'nombre_prestatario'
        },
        {
            'Text': '¿Cuál es el monto del préstamo?',
            'Alias': 'monto_prestamo'
        },
        {
            'Text': '¿Cuál es la tasa de interés anual?',
            'Alias': 'tasa_interes'
        },
        {
            'Text': '¿Cuál es la cuota mensual?',
            'Alias': 'cuota_mensual'
        },
        {
            'Text': '¿Cuál es la fecha del contrato?',
            'Alias': 'fecha_contrato'
        },
        {
            'Text': '¿Cuál es el plazo del préstamo en meses?',
            'Alias': 'plazo_meses'
        },
        {
            'Text': '¿Cuál es el nombre del banco?',
            'Alias': 'nombre_banco'
        }
    ]
    
    # Validar queries antes de retornar
    valid_queries = []
    for query in queries:
        if query.get('Text') and query.get('Alias'):
            if query['Text'].strip() and query['Alias'].strip():
                valid_queries.append(query)
    
    logger.info(f"📋 Queries de préstamo validadas: {len(valid_queries)}/{len(queries)}")
    return valid_queries

def get_id_document_queries():
    """
    VERSIÓN MEJORADA: Queries validadas para documentos de identidad
    """
    queries = [
        {
            'Text': '¿Cuál es el nombre completo de la persona?',
            'Alias': 'nombre_completo'
        },
        {
            'Text': '¿Cuál es el número del documento?',
            'Alias': 'numero_documento'
        },
        {
            'Text': '¿Cuál es la fecha de nacimiento?',
            'Alias': 'fecha_nacimiento'
        },
        {
            'Text': '¿Cuál es la fecha de expedición?',
            'Alias': 'fecha_expedicion'
        },
        {
            'Text': '¿Cuál es la fecha de caducidad?',
            'Alias': 'fecha_caducidad'
        },
        {
            'Text': '¿Cuál es la nacionalidad?',
            'Alias': 'nacionalidad'
        },
        {
            'Text': '¿Cuál es el sexo?',
            'Alias': 'sexo'
        },
        {
            'Text': '¿Cuál es el lugar de nacimiento?',
            'Alias': 'lugar_nacimiento'
        }
    ]
    
    # Validar queries
    valid_queries = []
    for query in queries:
        if query.get('Text') and query.get('Alias'):
            if query['Text'].strip() and query['Alias'].strip():
                valid_queries.append(query)
    
    logger.info(f"🆔 Queries de ID validadas: {len(valid_queries)}/{len(queries)}")
    return valid_queries

def get_generic_contract_queries():
    """
    VERSIÓN MEJORADA: Queries genéricas validadas
    """
    queries = [
        {
            'Text': '¿Cuáles son las partes del contrato?',
            'Alias': 'partes_contrato'
        },
        {
            'Text': '¿Cuál es la fecha del contrato?',
            'Alias': 'fecha_contrato'
        },
        {
            'Text': '¿Cuál es el objeto del contrato?',
            'Alias': 'objeto_contrato'
        },
        {
            'Text': '¿Cuál es el valor del contrato?',
            'Alias': 'valor_contrato'
        }
    ]
    
    # Validar queries
    valid_queries = []
    for query in queries:
        if query.get('Text') and query.get('Alias'):
            if query['Text'].strip() and query['Alias'].strip():
                valid_queries.append(query)
    
    logger.info(f"📄 Queries genéricas validadas: {len(valid_queries)}/{len(queries)}")
    return valid_queries

def get_credit_card_queries():
    """
    Queries para contratos de tarjeta de crédito
    """
    return [
        {
            'Text': '¿Cuál es el tipo de tarjeta de crédito?',
            'Alias': 'tipo_tarjeta'
        },
        {
            'Text': '¿Cuál es el límite de crédito aprobado?',
            'Alias': 'limite_credito'
        },
        {
            'Text': '¿Cuál es la tasa de interés anual de la tarjeta?',
            'Alias': 'tasa_tarjeta'
        },
        {
            'Text': '¿Cuál es la cuota de manejo anual?',
            'Alias': 'cuota_manejo_anual'
        },
        {
            'Text': '¿Cuál es el porcentaje de pago mínimo mensual?',
            'Alias': 'pago_minimo'
        },
        {
            'Text': '¿Cuál es la comisión por avance en efectivo?',
            'Alias': 'comision_avance'
        }
    ]

def get_account_contract_queries():
    """
    VERSIÓN MEJORADA: Queries para contratos de cuenta
    """
    queries = [
        {
            'Text': '¿Cuál es el tipo de cuenta?',
            'Alias': 'tipo_cuenta'
        },
        {
            'Text': '¿Cuál es el número de cuenta?',
            'Alias': 'numero_cuenta'
        },
        {
            'Text': '¿Cuál es el nombre del titular?',
            'Alias': 'titular_cuenta'
        },
        {
            'Text': '¿Cuál es el monto de apertura?',
            'Alias': 'monto_apertura'
        },
        {
            'Text': '¿Cuál es la comisión de mantenimiento?',
            'Alias': 'comision_mantenimiento'
        }
    ]
    
    # Validar queries
    valid_queries = []
    for query in queries:
        if query.get('Text') and query.get('Alias'):
            if query['Text'].strip() and query['Alias'].strip():
                valid_queries.append(query)
    
    logger.info(f"🏦 Queries de cuenta validadas: {len(valid_queries)}/{len(queries)}")
    return valid_queries