import os
from datetime import datetime
import logging
import re
 
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

def validate_textract_query(query, index=None):
    """
    Valida una query individual para Textract
    """
    try:
        # Verificar estructura básica
        if not isinstance(query, dict):
            return False, f"Query {index}: No es un diccionario"
        
        # Verificar campos requeridos
        if 'Text' not in query:
            return False, f"Query {index}: Falta campo 'Text'"
        
        if 'Alias' not in query:
            return False, f"Query {index}: Falta campo 'Alias'"
        
        text = query['Text']
        alias = query['Alias']
        
        # Verificar tipos
        if not isinstance(text, str):
            return False, f"Query {index}: 'Text' debe ser string"
        
        if not isinstance(alias, str):
            return False, f"Query {index}: 'Alias' debe ser string"
        
        # Verificar contenido
        if not text.strip():
            return False, f"Query {index}: 'Text' está vacío"
        
        if not alias.strip():
            return False, f"Query {index}: 'Alias' está vacío"
        
        # Verificar longitudes (límites de AWS Textract)
        if len(text) > 200:
            return False, f"Query {index}: 'Text' excede 200 caracteres ({len(text)})"
        
        if len(alias) > 100:
            return False, f"Query {index}: 'Alias' excede 100 caracteres ({len(alias)})"
        
        # ✅ VERIFICACIÓN MÁS ESTRICTA: Solo caracteres básicos para Textract
        if not re.match(r'^[a-zA-Z0-9_\-]+$', alias):
            return False, f"Query {index}: 'Alias' contiene caracteres no permitidos"
        
        return True, "Válida"
        
    except Exception as e:
        return False, f"Query {index}: Error de validación: {str(e)}"

def normalize_textract_queries(queries):
    """
    Limpia y normaliza queries para Textract
    ✅ VERSIÓN ULTRA ROBUSTA: Elimina todos los caracteres problemáticos
    """
    try:
        if not queries or not isinstance(queries, list):
            return []
        
        normalized_queries = []
        used_aliases = set()
        
        for i, query in enumerate(queries):
            try:
                if not isinstance(query, dict):
                    continue
                
                text = query.get('Text', '').strip()
                alias = query.get('Alias', '').strip()
                
                if not text or not alias:
                    continue
                
                # ✅ LIMPIAR TEXTO: Eliminar acentos y caracteres especiales
                text = remove_accents_and_special_chars(text)
                text = re.sub(r'\s+', ' ', text)  # Múltiples espacios -> uno
                text = text[:200]  # Limitar longitud
                
                # ✅ LIMPIAR ALIAS: Solo caracteres alfanuméricos y guiones bajos
                alias = remove_accents_and_special_chars(alias)
                alias = re.sub(r'[^\w]', '_', alias)  # Todo excepto alfanumérico -> _
                alias = re.sub(r'_+', '_', alias)  # Múltiples _ -> uno
                alias = alias.strip('_')[:100]  # Limpiar y limitar
                
                # Evitar alias duplicados
                original_alias = alias
                counter = 1
                while alias in used_aliases:
                    alias = f"{original_alias}_{counter}"
                    counter += 1
                
                if alias and text:  # Solo agregar si ambos son válidos
                    normalized_queries.append({
                        'Text': text,
                        'Alias': alias
                    })
                    used_aliases.add(alias)
                
            except Exception as query_error:
                logger.warning(f"Error normalizando query {i}: {str(query_error)}")
                continue
        
        return normalized_queries
        
    except Exception as e:
        logger.error(f"Error normalizando queries: {str(e)}")
        return []

def remove_accents_and_special_chars(text):
    """
    ✅ NUEVA FUNCIÓN: Elimina acentos y caracteres especiales
    """
    try:
        if not text:
            return ""
        
        # Mapeo de caracteres con acentos a sin acentos
        accent_mapping = {
            'á': 'a', 'à': 'a', 'ä': 'a', 'â': 'a', 'ā': 'a', 'ã': 'a',
            'é': 'e', 'è': 'e', 'ë': 'e', 'ê': 'e', 'ē': 'e',
            'í': 'i', 'ì': 'i', 'ï': 'i', 'î': 'i', 'ī': 'i',
            'ó': 'o', 'ò': 'o', 'ö': 'o', 'ô': 'o', 'ō': 'o', 'õ': 'o',
            'ú': 'u', 'ù': 'u', 'ü': 'u', 'û': 'u', 'ū': 'u',
            'ñ': 'n', 'ç': 'c',
            'Á': 'A', 'À': 'A', 'Ä': 'A', 'Â': 'A', 'Ā': 'A', 'Ã': 'A',
            'É': 'E', 'È': 'E', 'Ë': 'E', 'Ê': 'E', 'Ē': 'E',
            'Í': 'I', 'Ì': 'I', 'Ï': 'I', 'Î': 'I', 'Ī': 'I',
            'Ó': 'O', 'Ò': 'O', 'Ö': 'O', 'Ô': 'O', 'Ō': 'O', 'Õ': 'O',
            'Ú': 'U', 'Ù': 'U', 'Ü': 'U', 'Û': 'U', 'Ū': 'U',
            'Ñ': 'N', 'Ç': 'C',
            '¿': '', '¡': '', '?': '', '!': '.'
        }
        
        # Aplicar mapeo
        clean_text = text
        for accented, clean in accent_mapping.items():
            clean_text = clean_text.replace(accented, clean)
        
        return clean_text
        
    except Exception as e:
        logger.error(f"Error removiendo acentos: {str(e)}")
        return text

def get_loan_contract_queries():
    """
    ✅ VERSIÓN FINAL SIN ACENTOS: Queries para contratos de préstamo
    """
    queries = [
        {
            'Text': 'Cual es el numero del contrato',
            'Alias': 'numero_contrato'
        },
        {
            'Text': 'Cual es el nombre completo del prestatario',
            'Alias': 'nombre_prestatario'
        },
        {
            'Text': 'Cual es el monto del prestamo',
            'Alias': 'monto_prestamo'
        },
        {
            'Text': 'Cual es la tasa de interes anual',
            'Alias': 'tasa_interes'
        },
        {
            'Text': 'Cual es la cuota mensual',
            'Alias': 'cuota_mensual'
        },
        {
            'Text': 'Cual es la fecha del contrato',
            'Alias': 'fecha_contrato'
        },
        {
            'Text': 'Cual es el plazo del prestamo en meses',
            'Alias': 'plazo_meses'
        },
        {
            'Text': 'Cual es el nombre del banco',
            'Alias': 'nombre_banco'
        }
    ]
    
    # ✅ NORMALIZAR Y VALIDAR
    normalized_queries = normalize_textract_queries(queries)
    
    # Validar cada query normalizada
    valid_queries = []
    for i, query in enumerate(normalized_queries):
        is_valid, message = validate_textract_query(query, i)
        if is_valid:
            valid_queries.append(query)
        else:
            logger.warning(f"Query préstamo {i} inválida: {message}")
    
    logger.info(f"📋 Queries de préstamo procesadas: {len(valid_queries)}/{len(queries)} válidas")
    
    # Log de queries finales para debug
    for i, q in enumerate(valid_queries):
        logger.debug(f"  Préstamo Query {i+1}: {q['Alias']} = '{q['Text']}'")
    
    return valid_queries

def get_id_document_queries():
    """
    ✅ VERSIÓN FINAL SIN ACENTOS: Queries para documentos de identidad
    """
    queries = [
        {
            'Text': 'What is the full name',
            'Alias': 'nombre_completo'
        },
        {
            'Text': 'What is the full last name',
            'Alias': 'apellido_completo'
        },
        {
            'Text': 'Cual es la fecha de nacimiento',
            'Alias': 'fecha_nacimiento'
        },
        {
            'Text': 'Cual es la fecha de expedicion',
            'Alias': 'fecha_expedicion'
        },
        {
            'Text': 'Cual es la fecha de caducidad',
            'Alias': 'fecha_caducidad'
        },
        {
            'Text': 'Cual es la nacionalidad',
            'Alias': 'nacionalidad'
        },
        {
            'Text': 'Cual es el sexo',
            'Alias': 'sexo'
        },
        {
            'Text': 'Cual es el lugar de nacimiento',
            'Alias': 'lugar_nacimiento'
        }
    ]
    
    # ✅ NORMALIZAR Y VALIDAR
    normalized_queries = normalize_textract_queries(queries)
    
    valid_queries = []
    for i, query in enumerate(normalized_queries):
        is_valid, message = validate_textract_query(query, i)
        if is_valid:
            valid_queries.append(query)
        else:
            logger.warning(f"Query ID {i} inválida: {message}")
    
    logger.info(f"🆔 Queries de ID procesadas: {len(valid_queries)}/{len(queries)} válidas")
    
    for i, q in enumerate(valid_queries):
        logger.debug(f"  ID Query {i+1}: {q['Alias']} = '{q['Text']}'")
    
    return valid_queries

def get_account_contract_queries():
    """
    ✅ VERSIÓN FINAL SIN ACENTOS: Queries para contratos de cuenta
    """
    queries = [
        {
            'Text': 'Cual es el tipo de cuenta',
            'Alias': 'tipo_cuenta'
        },
        {
            'Text': 'Cual es el numero de cuenta',
            'Alias': 'numero_cuenta'
        },
        {
            'Text': 'Cual es el nombre del titular',
            'Alias': 'titular_cuenta'
        },
        {
            'Text': 'Cual es el monto de apertura',
            'Alias': 'monto_apertura'
        },
        {
            'Text': 'Cual es la comision de mantenimiento',
            'Alias': 'comision_mantenimiento'
        }
    ]
    
    # ✅ NORMALIZAR Y VALIDAR
    normalized_queries = normalize_textract_queries(queries)
    
    valid_queries = []
    for i, query in enumerate(normalized_queries):
        is_valid, message = validate_textract_query(query, i)
        if is_valid:
            valid_queries.append(query)
        else:
            logger.warning(f"Query cuenta {i} inválida: {message}")
    
    logger.info(f"🏦 Queries de cuenta procesadas: {len(valid_queries)}/{len(queries)} válidas")
    
    for i, q in enumerate(valid_queries):
        logger.debug(f"  Cuenta Query {i+1}: {q['Alias']} = '{q['Text']}'")
    
    return valid_queries

def get_generic_contract_queries():
    """
    ✅ VERSIÓN FINAL SIN ACENTOS: Queries genéricas
    """
    queries = [
        {
            'Text': 'Cuales son las partes del contrato',
            'Alias': 'partes_contrato'
        },
        {
            'Text': 'Cual es la fecha del contrato',
            'Alias': 'fecha_contrato'
        },
        {
            'Text': 'Cual es el objeto del contrato',
            'Alias': 'objeto_contrato'
        },
        {
            'Text': 'Cual es el valor del contrato',
            'Alias': 'valor_contrato'
        }
    ]
    
    # ✅ NORMALIZAR Y VALIDAR
    normalized_queries = normalize_textract_queries(queries)
    
    valid_queries = []
    for i, query in enumerate(normalized_queries):
        is_valid, message = validate_textract_query(query, i)
        if is_valid:
            valid_queries.append(query)
        else:
            logger.warning(f"Query genérica {i} inválida: {message}")
    
    logger.info(f"📄 Queries genéricas procesadas: {len(valid_queries)}/{len(queries)} válidas")
    
    for i, q in enumerate(valid_queries):
        logger.debug(f"  Genérica Query {i+1}: {q['Alias']} = '{q['Text']}'")
    
    return valid_queries

def get_credit_card_queries():
    """
    ✅ VERSIÓN FINAL SIN ACENTOS: Queries para tarjetas de crédito
    """
    queries = [
        {
            'Text': 'Cual es el tipo de tarjeta de credito',
            'Alias': 'tipo_tarjeta'
        },
        {
            'Text': 'Cual es el limite de credito aprobado',
            'Alias': 'limite_credito'
        },
        {
            'Text': 'Cual es la tasa de interes anual de la tarjeta',
            'Alias': 'tasa_tarjeta'
        },
        {
            'Text': 'Cual es la cuota de manejo anual',
            'Alias': 'cuota_manejo_anual'
        },
        {
            'Text': 'Cual es el porcentaje de pago minimo mensual',
            'Alias': 'pago_minimo'
        },
        {
            'Text': 'Cual es la comision por avance en efectivo',
            'Alias': 'comision_avance'
        }
    ]
    
    # ✅ NORMALIZAR Y VALIDAR
    normalized_queries = normalize_textract_queries(queries)
    
    valid_queries = []
    for i, query in enumerate(normalized_queries):
        is_valid, message = validate_textract_query(query, i)
        if is_valid:
            valid_queries.append(query)
        else:
            logger.warning(f"Query tarjeta {i} inválida: {message}")
    
    logger.info(f"💳 Queries de tarjeta procesadas: {len(valid_queries)}/{len(queries)} válidas")
    
    for i, q in enumerate(valid_queries):
        logger.debug(f"  Tarjeta Query {i+1}: {q['Alias']} = '{q['Text']}'")
    
    return valid_queries