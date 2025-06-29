# src/processors/contract_processor/app.py - VERSIÓN CORREGIDA
import os
import json
import boto3
import logging
import sys
import time
from datetime import datetime
import traceback
import uuid
import re

# Agregar las rutas para importar módulos comunes
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
sys.path.append('/opt')

from common.db_connector import (
    update_document_processing_status,
    get_document_by_id,
    generate_uuid,
    assign_folder_and_link,
    get_client_id_by_document,
    log_document_processing_start,
    log_document_processing_end,
    register_bank_contract_enhanced,
    execute_query
)

from contract_parser import (
    validate_contract_data_enhanced, 
    format_date_enhanced, 
    generate_contract_summary,
    extract_contract_data_from_queries_enhanced
)

from common.flow_utilis import crear_instancia_flujo_documento

# Configurar el logger
logger = logging.getLogger()
logger.setLevel(os.environ.get('LOG_LEVEL', 'INFO'))

# Configuración de reintentos para clientes AWS
from botocore.config import Config
retry_config = Config(
    retries={
        'max_attempts': 3,
        'mode': 'standard'
    }
)

def get_extracted_data_from_db_fixed(document_id):
    """
    VERSIÓN CORREGIDA: Recupera TODOS los datos extraídos incluyendo query answers
    SIN modificar el análisis existente - SOLO lectura
    """
    try:
        start_time = time.time()
        logger.info(f"📥 Recuperando datos extraídos de la base de datos para {document_id}...")
        
        # 1. Obtener documento básico
        document_data = get_document_by_id(document_id)
        if not document_data:
            logger.error(f"❌ No se encontró el documento {document_id}")
            return None
        
        # 2. Obtener análisis más reciente con TODOS los datos
        analysis_query = """
        SELECT 
            id_analisis,
            texto_extraido,
            entidades_detectadas,
            metadatos_extraccion,
            estado_analisis,
            confianza_clasificacion,
            tipo_documento
        FROM analisis_documento_ia
        WHERE id_documento = %s
        ORDER BY fecha_analisis DESC
        LIMIT 1
        """
        
        analysis_results = execute_query(analysis_query, (document_id,))
        
        if not analysis_results:
            logger.error(f"❌ No se encontró análisis para documento {document_id}")
            return None
        
        analysis_data = analysis_results[0]
        logger.info(f"📊 Análisis encontrado: ID {analysis_data.get('id_analisis')}")
        
        # 3. Procesar datos extraídos del documento
        extracted_data = {}
        if document_data.get('datos_extraidos_ia'):
            try:
                if isinstance(document_data['datos_extraidos_ia'], dict):
                    extracted_data = document_data['datos_extraidos_ia']
                else:
                    extracted_data = json.loads(document_data['datos_extraidos_ia'])
                logger.info(f"📄 Datos del documento procesados: {len(extracted_data)} campos")
            except json.JSONDecodeError as e:
                logger.error(f"❌ Error decodificando datos_extraidos_ia: {str(e)}")
        
        # 4. ✅ CRÍTICO: Procesar entidades detectadas (query answers)
        query_answers = {}
        structured_answers = {}
        
        if analysis_data.get('entidades_detectadas'):
            try:
                if isinstance(analysis_data['entidades_detectadas'], str):
                    entities = json.loads(analysis_data['entidades_detectadas'])
                else:
                    entities = analysis_data['entidades_detectadas']
                
                if isinstance(entities, dict):
                    # Si las entidades son query answers estructuradas
                    for key, value in entities.items():
                        if isinstance(value, dict) and 'answer' in value:
                            query_answers[key] = value['answer']
                            structured_answers[key] = value
                        else:
                            query_answers[key] = str(value) if value else ''
                    
                    logger.info(f"🔍 Query answers extraídas: {len(query_answers)} respuestas")
                    for alias, answer in query_answers.items():
                        logger.info(f"   📝 {alias}: {answer}")
                
            except json.JSONDecodeError as e:
                logger.warning(f"⚠️ Error decodificando entidades: {str(e)}")
        
        # 5. Procesar metadatos de extracción
        metadata = {}
        if analysis_data.get('metadatos_extraccion'):
            try:
                if isinstance(analysis_data['metadatos_extraccion'], str):
                    metadata = json.loads(analysis_data['metadatos_extraccion'])
                else:
                    metadata = analysis_data['metadatos_extraccion']
                
                # Extraer query answers de metadatos si existen
                if 'query_answers' in metadata:
                    meta_queries = metadata['query_answers']
                    for key, value in meta_queries.items():
                        if key not in query_answers:  # No sobrescribir
                            if isinstance(value, dict) and 'answer' in value:
                                query_answers[key] = value['answer']
                            else:
                                query_answers[key] = str(value) if value else ''
                    
                    logger.info(f"🔍 Query answers adicionales de metadatos: {len(meta_queries)}")
                
            except json.JSONDecodeError as e:
                logger.warning(f"⚠️ Error decodificando metadatos: {str(e)}")
        
        # 6. Consolidar todos los datos
        consolidated_data = {
            'document_id': document_id,
            'analysis_id': analysis_data.get('id_analisis'),
            'document_data': document_data,
            'extracted_data': {
                # Datos básicos del documento
                'tipo_documento_detectado': analysis_data.get('tipo_documento'),
                'confianza_clasificacion': analysis_data.get('confianza_clasificacion'),
                'estado_analisis': analysis_data.get('estado_analisis'),
                
                # Texto completo
                'texto_completo': analysis_data.get('texto_extraido', ''),
                
                # ✅ CRÍTICO: Query answers (datos más importantes para contratos)
                'query_answers': query_answers,
                'structured_query_answers': structured_answers,
                
                # Datos extraídos del documento
                **extracted_data,
                
                # Metadatos adicionales
                'metadatos_extraccion': metadata
            }
        }
        
        # 7. Log de resumen
        logger.info(f"✅ Datos consolidados para {document_id}:")
        logger.info(f"   📝 Texto: {len(analysis_data.get('texto_extraido', '')) or 0} caracteres")
        logger.info(f"   🔍 Query answers: {len(query_answers)} respuestas")
        logger.info(f"   📊 Confianza: {analysis_data.get('confianza_clasificacion', 0):.2f}")
        logger.info(f"   ⏱️ Tiempo consulta: {time.time() - start_time:.2f}s")
        
        return consolidated_data
        
    except Exception as e:
        logger.error(f"❌ Error crítico recuperando datos: {str(e)}")
        import traceback
        logger.error(f"📍 Stack trace: {traceback.format_exc()}")
        return None

def process_contract_data_fixed(document_id, extracted_data):
    """
    VERSIÓN CORREGIDA: Procesa datos priorizando query answers
    NO modifica análisis - solo procesa datos para contratos_bancarios
    """
    try:
        start_time = time.time()
        logger.info(f"🔍 Procesando datos de contrato para documento {document_id}")
        
        # Verificar si tenemos query answers (prioritario)
        query_answers = extracted_data.get('query_answers', {})
        text_completo = extracted_data.get('texto_completo', '')
        
        if query_answers and len(query_answers) > 0:
            logger.info(f"🎯 Usando query answers como fuente principal ({len(query_answers)} respuestas)")
            # Usar la nueva función que prioriza query answers
            contract_data = extract_contract_data_from_queries_enhanced(query_answers, text_completo)
        else:
            logger.warning(f"⚠️ No hay query answers, usando análisis de texto tradicional")
            if not text_completo:
                logger.error(f"❌ No hay texto completo disponible para procesar")
                return {
                    'success': False,
                    'error': 'No hay datos disponibles para procesar (ni query answers ni texto)'
                }
            
            # Fallback al método tradicional
            contract_data = extract_contract_data_from_queries_enhanced({}, text_completo)
        
        # Complementar con datos ya extraídos si existen
        if extracted_data.get('specific_data'):
            for key, value in extracted_data['specific_data'].items():
                if not contract_data.get(key) and value:
                    contract_data[key] = value
        
        # Generar resumen del contrato
        contract_summary = generate_contract_summary(contract_data)
        contract_data['resumen_ejecutivo'] = contract_summary
        logger.info(f"📄 Resumen del contrato: {contract_summary}")
        
        # Agregar resumen a observaciones si no hay
        if not contract_data.get('observaciones'):
            contract_data['observaciones'] = f"Resumen: {contract_summary}"
        
        # Formatear fechas si están presentes
        for field in ['fecha_inicio', 'fecha_fin']:
            if contract_data.get(field):
                iso_date = format_date_enhanced(contract_data[field])
                if iso_date:
                    contract_data[f"{field}_iso"] = iso_date
        
        # Validar los datos extraídos
        validation = validate_contract_data_enhanced(contract_data)
        
        # Añadir metadatos de procesamiento
        contract_data['fuente'] = 'contract_processor_query_based' if query_answers else 'contract_processor_text_based'
        contract_data['tiempo_procesamiento'] = time.time() - start_time
        contract_data['fecha_procesamiento'] = datetime.now().isoformat()
        contract_data['confianza'] = validation['confidence']
        
        logger.info(f"✅ Procesamiento completado en {contract_data['tiempo_procesamiento']:.2f}s")
        logger.info(f"📊 Confianza: {validation['confidence']:.2f}")
        logger.info(f"🎯 Fuente: {contract_data['fuente']}")
        
        return {
            'success': True,
            'contract_data': contract_data,
            'validation': validation
        }
        
    except Exception as e:
        logger.error(f"❌ Error al procesar datos de contrato {document_id}: {str(e)}")
        logger.error(traceback.format_exc())
        return {
            'success': False,
            'error': str(e)
        }

def evaluate_confidence_simple(confidence_score, validation_results):
    """Función simple para evaluar si requiere revisión manual"""
    requires_review = False
    
    # Requiere revisión si confianza es baja
    if confidence_score < 0.7:
        requires_review = True
    
    # Requiere revisión si hay errores críticos
    if validation_results.get('errors') and len(validation_results['errors']) > 0:
        requires_review = True
    
    # Requiere revisión si hay muchas advertencias
    if validation_results.get('warnings') and len(validation_results['warnings']) > 2:
        requires_review = True
    
    return requires_review


def lambda_handler(event, context):
    """
    VERSIÓN SIMPLIFICADA: Procesa contratos sin modificar análisis existente
    - Lee datos del análisis existente
    - Guarda en tabla contratos_bancarios
    - SIEMPRE asigna carpeta
    - NO modifica el análisis original
    """
    start_time = time.time()
    logger.info("="*80)
    logger.info("🚀 PROCESADOR DE CONTRATOS - VERSIÓN SIMPLIFICADA")
    logger.info("="*80)
    logger.info("Evento recibido: " + json.dumps(event))
    
    response = {
        'procesados': 0,
        'errores': 0,
        'requieren_revision': 0,
        'carpetas_asignadas': 0,
        'detalles': []
    }

    for record in event['Records']:
        documento_detalle = {
            'documento_id': None,
            'estado': 'sin_procesar',
            'tiempo': 0,
            'tipo_detectado': None,
            'datos_guardados': False,
            'carpeta_asignada': False,
            'requiere_revision': False
        }
        
        record_start = time.time()
        registro_id = None
        document_id = None
        
        try:
            # Parsear mensaje
            message_body = json.loads(record['body'])
            document_id = message_body['document_id']
            documento_detalle['documento_id'] = document_id
            
            logger.info(f"📄 Procesando contrato {document_id}")
            
            # Iniciar registro de procesamiento
            registro_id = log_document_processing_start(
                document_id, 
                'procesamiento_contrato_simplificado',
                datos_entrada=message_body
            )
            
            # ✅ PASO 1: Obtener datos de la BD (SIN MODIFICAR ANÁLISIS)
            logger.info(f"📥 Recuperando datos extraídos de la base de datos...")
            document_data_result = get_extracted_data_from_db_fixed(document_id)
            
            if not document_data_result:
                raise Exception(f"No se pudieron recuperar datos del documento {document_id}")
            
            # Obtener tipo detectado si existe
            tipo_detectado = document_data_result['extracted_data'].get('tipo_documento_detectado', 'contrato')
            documento_detalle['tipo_detectado'] = tipo_detectado
            
            # ✅ PASO 2: Procesar datos del contrato
            logger.info(f"🔍 Procesando datos del contrato...")
            process_result = process_contract_data_fixed(
                document_id, 
                document_data_result['extracted_data']
            )
            
            if not process_result['success']:
                raise Exception(f"Error al procesar datos de contrato: {process_result.get('error')}")
            
            contract_data = process_result['contract_data']
            validation = process_result['validation']
            
            logger.info(f"📊 Validación completada - Confianza: {validation['confidence']:.2f}")
            
            # ✅ PASO 3: Evaluar si requiere revisión manual
            requires_review = evaluate_confidence_simple(
                validation['confidence'],
                validation
            )
            
            documento_detalle['requiere_revision'] = requires_review
            
            if requires_review:
                response['requieren_revision'] += 1
                logger.warning(f"⚠️ Documento {document_id} requiere revisión manual")
            
            # ✅ PASO 4: Guardar en tabla contratos_bancarios (si hay datos suficientes)
            should_save = True
            if not contract_data.get('numero_contrato'):
                logger.warning(f"⚠️ No se encontró número de contrato - intentando guardar de todos modos")
                # Generar número temporal si no existe
                if not contract_data.get('numero_contrato'):
                    contract_data['numero_contrato'] = f"TEMP-{document_id[:8].upper()}"
                    logger.info(f"📝 Número de contrato temporal generado: {contract_data['numero_contrato']}")
            
            if should_save:
                logger.info(f"💾 Guardando datos del contrato en tabla contratos_bancarios...")
                
                # Agregar observaciones basadas en la validación
                observaciones = []
                if validation.get('warnings'):
                    observaciones.extend(validation['warnings'])
                if validation.get('errors'):
                    observaciones.extend([f"ERROR: {e}" for e in validation['errors']])
                if requires_review:
                    observaciones.append("REQUIERE REVISIÓN MANUAL")
                
                if observaciones:
                    existing_obs = contract_data.get('observaciones', '')
                    new_obs = '; '.join(observaciones)
                    contract_data['observaciones'] = f"{existing_obs}; {new_obs}" if existing_obs else new_obs
                
                # Registrar en tabla contratos_bancarios
                success = register_bank_contract_enhanced(document_id, contract_data)
                
                if success:
                    logger.info(f"✅ Contrato guardado exitosamente en contratos_bancarios")
                    documento_detalle['datos_guardados'] = True
                    response['procesados'] += 1
                else:
                    logger.error(f"❌ Error al guardar contrato en tabla específica")
                    documento_detalle['estado'] = 'error_guardado'
                    response['errores'] += 1
            else:
                logger.warning(f"⚠️ Datos insuficientes para guardar en contratos_bancarios")
                documento_detalle['estado'] = 'datos_insuficientes'
                response['requieren_revision'] += 1
            
            # ✅ PASO 5: SIEMPRE ASIGNAR CARPETA (CRÍTICO SEGÚN REQUISITOS)
            logger.info(f"📁 Asignando carpeta para documento {document_id}...")
            
            try:
                # Buscar cliente del documento
                cliente_id = get_client_id_by_document(document_id)
                
                if cliente_id:
                    logger.info(f"👤 Cliente encontrado: {cliente_id}")
                    folder_result = assign_folder_and_link(cliente_id, document_id)
                    
                    if folder_result:
                        logger.info(f"✅ Carpeta asignada correctamente para documento {document_id}")
                        documento_detalle['carpeta_asignada'] = True
                        response['carpetas_asignadas'] += 1
                    else:
                        logger.warning(f"⚠️ No se pudo asignar carpeta para documento {document_id}")
                else:
                    logger.warning(f"⚠️ No se encontró cliente para documento {document_id}")
                    # Intentar crear vínculo genérico o buscar por datos del contrato
                    # Esto se podría expandir según tus reglas de negocio
                    
            except Exception as folder_error:
                logger.error(f"❌ Error asignando carpeta: {str(folder_error)}")
                # No fallar por error de carpeta, solo advertir
            
            # ✅ PASO 6: Actualizar estado del documento
            if requires_review:
                status = 'requiere_revision_manual'
                message = "Contrato procesado - Requiere revisión manual"
            elif validation['is_valid'] and documento_detalle['datos_guardados']:
                status = 'procesamiento_completado'
                message = "Contrato bancario procesado y guardado correctamente"
            elif documento_detalle['datos_guardados']:
                status = 'procesado_con_advertencias'
                message = "Contrato procesado con advertencias"
            else:
                status = 'procesado_sin_guardar'
                message = "Contrato procesado pero no guardado en tabla específica"
            
            final_details = {
                'validación': validation,
                'tipo_contrato': contract_data.get('tipo_contrato'),
                'numero_contrato': contract_data.get('numero_contrato'),
                'campos_extraídos': [k for k, v in contract_data.items() if v is not None],
                'requires_review': requires_review,
                'datos_guardados': documento_detalle['datos_guardados'],
                'carpeta_asignada': documento_detalle['carpeta_asignada'],
                'procesador': 'contract_processor_simplificado'
            }
            
            update_document_processing_status(
                document_id, 
                status, 
                json.dumps(final_details, ensure_ascii=False)
            )
            
            documento_detalle['confianza'] = validation['confidence']
            documento_detalle['estado_final'] = status
            documento_detalle['estado'] = 'procesado'
            
            # Finalizar registro principal
            log_document_processing_end(
                registro_id, 
                estado='completado',
                confianza=validation['confidence'],
                datos_salida=final_details,
                mensaje_error=None if validation['is_valid'] else "Procesado con advertencias"
            )
            
            # ==================== PUBLICAR EVENTO ====================
            crear_instancia_flujo_documento(document_id)
                
            logger.info(f"✅ Documento {document_id} procesado completamente")
            logger.info(f"   📋 Tipo contrato: {contract_data.get('tipo_contrato')}")
            logger.info(f"   📊 Confianza: {validation['confidence']:.2f}")
            logger.info(f"   📝 Estado: {status}")
            logger.info(f"   📁 Carpeta asignada: {'Sí' if documento_detalle['carpeta_asignada'] else 'No'}")
            logger.info(f"   💾 Datos guardados: {'Sí' if documento_detalle['datos_guardados'] else 'No'}")
                
        except Exception as e:
            error_msg = str(e)
            logger.error(f"❌ Error procesando contrato {document_id if document_id else 'DESCONOCIDO'}: {error_msg}")
            logger.error(traceback.format_exc())
            
            documento_detalle['estado'] = 'error'
            documento_detalle['error'] = error_msg
            response['errores'] += 1
            
            # Actualizar estado de error
            if document_id:
                try:
                    update_document_processing_status(
                        document_id, 
                        'error_procesamiento_contrato',
                        f"Error en procesamiento de contrato: {error_msg}"
                    )
                except:
                    pass
            
            # Finalizar registro con error
            if registro_id:
                log_document_processing_end(
                    registro_id, 
                    estado='error',
                    mensaje_error=error_msg
                )
                
        finally:
            # Calcular tiempo de procesamiento
            tiempo_procesamiento = time.time() - record_start
            documento_detalle['tiempo'] = tiempo_procesamiento
            response['detalles'].append(documento_detalle)

    # Resumen final
    total_time = time.time() - start_time
    response['tiempo_total'] = total_time
    response['total_registros'] = len(event['Records'])
    
    logger.info("="*80)
    logger.info("📊 RESUMEN DEL PROCESAMIENTO DE CONTRATOS")
    logger.info("="*80)
    logger.info(f"✅ Contratos procesados exitosamente: {response['procesados']}")
    logger.info(f"⚠️ Contratos que requieren revisión: {response['requieren_revision']}")
    logger.info(f"❌ Contratos con errores: {response['errores']}")
    logger.info(f"📁 Carpetas asignadas: {response['carpetas_asignadas']}")
    logger.info(f"⏱️ Tiempo total: {total_time:.2f} segundos")
    
    return {
        'statusCode': 200,
        'body': json.dumps(response, ensure_ascii=False)
    }