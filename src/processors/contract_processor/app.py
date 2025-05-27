# src/processors/contract_processor/app.py
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
from common.confidence_utils import evaluate_confidence, mark_for_manual_review

# Agregar las rutas para importar módulos comunes
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
sys.path.append('/opt')

from common.db_connector import (
    update_document_extraction_data,
    update_document_processing_status,
    get_document_by_id,
    insert_analysis_record,
    update_analysis_record,
    generate_uuid,
    link_document_to_client,
    assign_folder_and_link,
    get_client_id_by_document,
    log_document_processing_start,
    log_document_processing_end,
    log_contract_changes,
    register_bank_contract
)
 
from contract_parser import extract_contract_data, validate_contract_data, format_date, extract_clauses_by_section, generate_contract_summary

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

# Instanciar clientes de AWS con reintentos
sqs_client = boto3.client('sqs', config=retry_config)

# Obtener variables de entorno
CONTRACT_PROCESSOR_QUEUE_URL = os.environ.get('CONTRACT_PROCESSOR_QUEUE_URL', '')

def operation_with_retry(operation_func, max_retries=3, base_delay=0.5, **kwargs):
    """Ejecuta una operación con reintentos en caso de fallos"""
    last_exception = None
    for attempt in range(max_retries):
        try:
            return operation_func(**kwargs)
        except Exception as e:
            last_exception = e
            delay = base_delay * (2 ** attempt)
            logger.warning(f"Reintento {attempt+1}/{max_retries} después de error: {str(e)}. Esperando {delay:.2f}s")
            time.sleep(delay)
    
    # Si llegamos aquí, todos los reintentos fallaron
    logger.error(f"Operación falló después de {max_retries} intentos. Último error: {str(last_exception)}")
    raise last_exception

def get_extracted_data_from_db(document_id):
    """
    Recupera los datos ya extraídos por textract_callback de la base de datos.
    Centraliza la extracción para simplificar el manejo de errores.
    """
    try:
        start_time = time.time()
        # Obtener documento
        document_data = get_document_by_id(document_id)
        
        if not document_data:
            logger.error(f"No se encontró el documento {document_id} en la base de datos")
            return None
        
        # Obtener los datos extraídos del campo JSON
        extracted_data = {}
        if document_data.get('datos_extraidos_ia'):
            try:
                # Si ya es un diccionario, usarlo directamente
                if isinstance(document_data['datos_extraidos_ia'], dict):
                    extracted_data = document_data['datos_extraidos_ia']
                else:
                    # Si es una cadena JSON, deserializarla
                    extracted_data = json.loads(document_data['datos_extraidos_ia'])
            except json.JSONDecodeError:
                logger.error(f"Error al decodificar datos_extraidos_ia para documento {document_id}")
                return None
        
        # Obtener texto extraído y datos analizados
        query = """
        SELECT texto_extraido, entidades_detectadas, metadatos_extraccion, estado_analisis
        FROM analisis_documento_ia
        WHERE id_documento = %s
        ORDER BY fecha_analisis DESC
        LIMIT 1
        """
        
        from common.db_connector import execute_query
        analysis_results = execute_query(query, (document_id,))
        
        if not analysis_results:
            logger.warning(f"No se encontró análisis en base de datos para documento {document_id}")
            # Continuar con lo que tengamos en datos_extraidos_ia
        else:
            analysis_data = analysis_results[0]
            
            # Agregar texto completo y entidades detectadas a los datos extraídos
            if analysis_data.get('texto_extraido'):
                extracted_data['texto_completo'] = analysis_data['texto_extraido']
            
            if analysis_data.get('entidades_detectadas'):
                try:
                    entidades = json.loads(analysis_data['entidades_detectadas']) if isinstance(analysis_data['entidades_detectadas'], str) else analysis_data['entidades_detectadas']
                    extracted_data['entidades'] = entidades
                except json.JSONDecodeError:
                    logger.warning(f"Error al decodificar entidades_detectadas para documento {document_id}")
            
            # Agregar metadatos de extracción
            if analysis_data.get('metadatos_extraccion'):
                try:
                    metadatos = json.loads(analysis_data['metadatos_extraccion']) if isinstance(analysis_data['metadatos_extraccion'], str) else analysis_data['metadatos_extraccion']
                    extracted_data['metadatos_extraccion'] = metadatos
                except json.JSONDecodeError:
                    logger.warning(f"Error al decodificar metadatos_extraccion para documento {document_id}")
        
        # Registrar tiempo de consulta
        logger.info(f"Datos recuperados para documento {document_id} en {time.time() - start_time:.2f} segundos")
        
        return {
            'document_id': document_id,
            'document_data': document_data,
            'extracted_data': extracted_data
        }
        
    except Exception as e:
        logger.error(f"Error al recuperar datos de documento {document_id}: {str(e)}")
        logger.error(traceback.format_exc())
        return None

def process_contract_data(document_id, extracted_data):
    """
    Procesa los datos ya extraídos del contrato para estructurarlos según nuestro modelo.
    Esta función no llama a servicios externos como Textract.
    """
    try:
        start_time = time.time()
        logger.info(f"Procesando datos de contrato para documento {document_id}")
        
        # Verificar si tenemos el texto completo
        if not extracted_data.get('texto_completo'):
            logger.warning(f"No se encontró texto completo para documento {document_id}")
            return {
                'success': False,
                'error': 'No hay texto completo disponible para procesar'
            }
        
        # Usar el texto completo ya extraído
        full_text = extracted_data['texto_completo']
        
        # Crear bloques de texto a partir del texto completo
        # Usamos saltos de línea como separadores
        text_blocks = full_text.split('\n')
        
        # Usar el parser de contratos para extraer datos estructurados
        contract_data = extract_contract_data(full_text, text_blocks)
        
        # Complementar con datos ya extraídos
        if extracted_data.get('specific_data'):
            # Si ya tenemos datos específicos, complementamos
            for key, value in extracted_data['specific_data'].items():
                if not contract_data.get(key) and value:
                    contract_data[key] = value
        
        # Añadir información de entidades si está disponible
        if extracted_data.get('entidades'):
            # Buscar entidades que puedan ser firmantes
            posibles_firmantes = []
            for entidad in extracted_data['entidades']:
                if isinstance(entidad, dict) and entidad.get('Type') == 'PERSON':
                    nombre = entidad.get('Text', '')
                    if nombre and nombre not in posibles_firmantes:
                        posibles_firmantes.append(nombre)
            
            # Si encontramos posibles firmantes, los añadimos a los existentes
            if posibles_firmantes:
                existing_firmantes = contract_data.get('firmantes', [])
                for firmante in posibles_firmantes:
                    if firmante not in existing_firmantes:
                        existing_firmantes.append(firmante)
                contract_data['firmantes'] = existing_firmantes
        
        # ❌ ELIMINAR ESTA LÍNEA - Ya se llamó arriba
        # contract_data = extract_contract_data(full_text, text_blocks)

        # ✅ ESTO ESTÁ BIEN - Generar resumen del contrato
        contract_summary = generate_contract_summary(contract_data)
        contract_data['resumen_ejecutivo'] = contract_summary
        logger.info(f"📄 Resumen del contrato: {contract_summary}")

        # ✅ ESTO ESTÁ BIEN - Agregar resumen a observaciones si no hay
        if not contract_data.get('observaciones'):
            contract_data['observaciones'] = f"Resumen: {contract_summary}"

        # Extraer cláusulas importantes y términos
        clause_sections = extract_clauses_by_section(full_text)
        if clause_sections:
            contract_data['secciones_clausulas'] = clause_sections
        
        # Formatear fechas si están presentes
        for field in ['fecha_inicio', 'fecha_fin']:
            if contract_data.get(field):
                iso_date = format_date(contract_data[field])
                if iso_date:
                    contract_data[f"{field}_iso"] = iso_date
        
        # Validar los datos extraídos
        validation = validate_contract_data(contract_data)
        
        # Añadir metadatos de procesamiento
        contract_data['fuente'] = 'contract_processor'
        contract_data['tiempo_procesamiento'] = time.time() - start_time
        contract_data['fecha_procesamiento'] = datetime.now().isoformat()
        contract_data['confianza'] = validation['confidence']
        
        logger.info(f"Procesamiento de contrato completado en {contract_data['tiempo_procesamiento']:.2f} segundos. Confianza: {validation['confidence']:.2f}")
        
        return {
            'success': True,
            'contract_data': contract_data,
            'validation': validation
        }
        
    except Exception as e:
        logger.error(f"Error al procesar datos de contrato {document_id}: {str(e)}")
        logger.error(traceback.format_exc())
        return {
            'success': False,
            'error': str(e)
        }

def save_contract_data_to_db(document_id, contract_data, validation, operation_type="procesamiento_normal"):
    """
    Guarda los datos de contrato extraídos en la base de datos de forma segura,
    gestionando errores y validando los datos antes de almacenarlos.
    """
    try:
        analysis_id = None  # Se inicializa para devolverlo al final

        # Verificar que contract_data sea JSON serializable
        try:
            json_data = json.dumps(contract_data)
        except (TypeError, OverflowError) as json_error:
            logger.error(f"Error al serializar datos del contrato: {str(json_error)}")
            
            # Filtrar y limpiar los datos problemáticos
            cleaned_data = {}
            for key, value in contract_data.items():
                try:
                    json.dumps({key: value})
                    cleaned_data[key] = value
                except:
                    logger.warning(f"Campo no serializable: {key}, convertido a string")
                    if value is None:
                        cleaned_data[key] = None
                    else:
                        try:
                            cleaned_data[key] = str(value)
                        except:
                            cleaned_data[key] = f"ERROR: No serializable ({type(value).__name__})"
            
            json_data = json.dumps(cleaned_data)
            contract_data = cleaned_data

        # Extraer valores para actualización
        confidence = validation.get('confidence', 0.0)
        is_valid = validation.get('is_valid', False)

        # Paso 1: Actualizar los datos extraídos en la tabla documentos
        update_document_extraction_data(
            document_id,
            json_data,
            confidence,
            is_valid
        )

        # Paso 2: Determinar estado
        status = 'completado' if is_valid else 'revisión_requerida'
        if not is_valid and len(validation.get('errors', [])) > 0:
            status = 'error_validacion'

        # Crear mensaje y detalles
        message_parts = []
        if is_valid:
            message_parts.append("Contrato bancario procesado correctamente")
        else:
            message_parts.append("Contrato procesado con advertencias")

        if validation.get('warnings'):
            message_parts.append(f"{len(validation.get('warnings'))} advertencias encontradas")
        if validation.get('errors'):
            message_parts.append(f"{len(validation.get('errors'))} errores críticos")

        message = ". ".join(message_parts)

        details = {
            "validación": {
                "confianza": confidence,
                "es_válido": is_valid,
                "advertencias": validation.get('warnings', []),
                "errores": validation.get('errors', [])
            },
            "tipo_operación": operation_type,
            "campos_extraídos": list(contract_data.keys())
        }

        try:
            update_document_processing_status(
                document_id, 
                status, 
                json.dumps(details)
            )
            logger.info(f"Estado actualizado para documento {document_id}: {status}")

            # Generar un nuevo ID de análisis aunque haya ido bien
            analysis_id = generate_uuid()
            insert_analysis_record({
                'id_analisis': analysis_id,
                'id_documento': document_id,
                'tipo_documento': 'contrato',
                'confianza_clasificacion': confidence,
                'texto_extraido': None,
                'entidades_detectadas': json.dumps(contract_data.get('firmantes', [])),
                'metadatos_extraccion': json.dumps(details),
                'fecha_analisis': datetime.now().isoformat(),
                'estado_analisis': status,
                'mensaje_error': None,
                'version_modelo': 'contract_processor-1.0',
                'tiempo_procesamiento': int(contract_data.get('tiempo_procesamiento', 0) * 1000),
                'procesado_por': 'contract_processor',
                'requiere_verificacion': not is_valid,
                'verificado': False,
                'verificado_por': None,
                'fecha_verificacion': None
            })

        except Exception as status_error:
            logger.error(f"Error al actualizar estado del documento: {str(status_error)}")

            try:
                analysis_id = generate_uuid()
                insert_analysis_record({
                    'id_analisis': analysis_id,
                    'id_documento': document_id,
                    'tipo_documento': 'contrato',
                    'confianza_clasificacion': confidence,
                    'texto_extraido': None,
                    'entidades_detectadas': json.dumps(contract_data.get('firmantes', [])),
                    'metadatos_extraccion': json.dumps(details),
                    'fecha_analisis': datetime.now().isoformat(),
                    'estado_analisis': status,
                    'mensaje_error': str(status_error),
                    'version_modelo': 'contract_processor-1.0',
                    'tiempo_procesamiento': int(contract_data.get('tiempo_procesamiento', 0) * 1000),
                    'procesado_por': 'contract_processor',
                    'requiere_verificacion': not is_valid,
                    'verificado': False,
                    'verificado_por': None,
                    'fecha_verificacion': None
                })
                logger.info(f"Nuevo registro de análisis creado con éxito para {document_id}")
            except Exception as recovery_error:
                logger.error(f"Error también en recuperación: {str(recovery_error)}")
                raise

        # Paso 3: Intentar vincular el documento con un cliente
        #try:
        #    if contract_data.get('firmantes'):
        #        for firmante in contract_data['firmantes']:
        #            if link_document_to_client(document_id, client_id=None, firmante=firmante):
        #                logger.info(f"Documento {document_id} vinculado a cliente mediante firmante: {firmante}")
        #                break

        #    if contract_data.get('numero_contrato') or contract_data.get('numero_producto'):
        #        reference = contract_data.get('numero_contrato') or contract_data.get('numero_producto')
        #        if link_document_to_client(document_id, client_id=None, reference=reference):
        #            logger.info(f"Documento {document_id} vinculado a cliente mediante referencia: {reference}")

        #    link_document_to_client(document_id)

        #except Exception as link_error:
        #    logger.warning(f"No se pudo vincular el documento a un cliente: {str(link_error)}")

        return {
            "success": True,
            "document_id": document_id,
            "status": status,
            "confidence": confidence,
            "is_valid": is_valid,
            "analysis_id": analysis_id  # ← ¡Aquí está!
        }

    except Exception as e:
        logger.error(f"Error grave al guardar datos del contrato: {str(e)}")
        logger.error(traceback.format_exc())
        try:
            error_msg = f"Error al guardar datos: {str(e)}"
            update_document_processing_status(document_id, 'error_guardado', error_msg)
        except:
            logger.error("No se pudo actualizar el estado con el error")
        return {
            "success": False,
            "document_id": document_id,
            "error": str(e)
        }


# Modificar la función lambda_handler en contract_processor/app.py
def lambda_handler(event, context):
    """
    Función principal que procesa contratos bancarios.
    VERSIÓN MEJORADA que registra en la tabla contratos_bancarios
    """
    start_time = time.time()
    logger.info("="*80)
    logger.info("🚀 INICIANDO PROCESAMIENTO DE CONTRATO BANCARIO")
    logger.info("="*80)
    logger.info("Evento recibido: " + json.dumps(event))
    
    response = {
        'procesados': 0,
        'errores': 0,
        'requieren_revision': 0,
        'detalles': []
    }

    for record in event['Records']:
        documento_detalle = {
            'documento_id': None,
            'estado': 'sin_procesar',
            'tiempo': 0,
            'tipo_detectado': None,
            'datos_extraidos': False
        }
        
        record_start = time.time()
        registro_id = None
        
        try:
            # Parsear mensaje
            message_body = json.loads(record['body'])
            document_id = message_body['document_id']
            documento_detalle['documento_id'] = document_id
            
            logger.info(f"📄 Procesando contrato {document_id}")
            
            # Iniciar registro de procesamiento
            registro_id = log_document_processing_start(
                document_id, 
                'procesamiento_contrato',
                datos_entrada=message_body
            )
            
            # Obtener datos de la BD
            logger.info(f"📥 Recuperando datos extraídos de la base de datos...")
            document_data_result = get_extracted_data_from_db(document_id)
            
            if not document_data_result:
                raise Exception(f"No se pudieron recuperar datos del documento {document_id}")
            
            # Obtener tipo detectado si existe
            tipo_detectado = document_data_result['extracted_data'].get('tipo_documento_detectado', 'contrato')
            documento_detalle['tipo_detectado'] = tipo_detectado
            
            # Procesar datos del contrato
            logger.info(f"🔍 Procesando datos del contrato...")
            process_result = process_contract_data(
                document_id, 
                document_data_result['extracted_data']
            )
            
            if not process_result['success']:
                raise Exception(f"Error al procesar datos de contrato: {process_result.get('error')}")
            
            contract_data = process_result['contract_data']
            validation = process_result['validation']
            
            logger.info(f"📊 Validación completada - Confianza: {validation['confidence']:.2f}")
            
            # Evaluar si requiere revisión manual
            requires_review = evaluate_confidence(
                validation['confidence'],
                document_type='contrato',
                validation_results=validation
            )
            
            if requires_review:
                documento_detalle['estado'] = 'requiere_revision'
                response['requieren_revision'] += 1
                logger.warning(f"⚠️ Documento {document_id} requiere revisión manual")
            
            # NUEVO: Registrar en tabla contratos_bancarios
            should_save = True
            if not contract_data.get('numero_contrato'):
                logger.warning(f"⚠️ No se encontró número de contrato")
                should_save = False
            
            if should_save:
                logger.info(f"💾 Guardando datos del contrato en tabla contratos_bancarios...")
                
                # Agregar observaciones basadas en la validación
                observaciones = []
                if validation.get('warnings'):
                    observaciones.extend(validation['warnings'])
                if validation.get('errors'):
                    observaciones.extend([f"ERROR: {e}" for e in validation['errors']])
                
                contract_data['observaciones'] = '; '.join(observaciones) if observaciones else None
                
                # Registrar en tabla contratos_bancarios
                success = register_bank_contract(document_id, contract_data)
                
                if success:
                    logger.info(f"✅ Contrato guardado exitosamente en contratos_bancarios")
                    documento_detalle['datos_extraidos'] = True
                    
                    # Mostrar cambios si los hay
                    log_contract_changes(document_id)
                else:
                    logger.error(f"❌ Error al guardar contrato en tabla específica")
                    documento_detalle['error_guardado'] = "Falló el guardado en contratos_bancarios"
            else:
                logger.warning(f"⚠️ Datos insuficientes para guardar en contratos_bancarios")
                documento_detalle['estado'] = 'datos_insuficientes'
                response['requieren_revision'] += 1
            
            # Guardar datos generales del documento
            save_result = save_contract_data_to_db(
                document_id,
                contract_data,
                validation,
                'procesamiento_optimizado'
            )
            
            if save_result['success']:
                logger.info(f"✅ Datos generales guardados correctamente para {document_id}")
                documento_detalle['estado'] = 'procesado_completo'
                documento_detalle['confianza'] = validation.get('confidence', 0.0)
                response['procesados'] += 1
                
                # Marcar para revisión manual si es necesario
                if requires_review:
                    mark_for_manual_review(
                        document_id=document_id,
                        analysis_id=save_result.get('analysis_id'),
                        confidence=validation['confidence'],
                        document_type='contrato',
                        validation_info=validation,
                        extracted_data=contract_data
                    )
            
            # Actualizar estado final
            if documento_detalle['estado'] == 'sin_procesar':
                documento_detalle['estado'] = 'procesado'
                
            # Determinar estado final
            if requires_review or not should_save:
                status = 'requiere_revision_manual'
                message = "Contrato procesado - Requiere revisión manual"
            elif validation['is_valid']:
                status = 'completado'
                message = "Contrato bancario procesado correctamente"
            else:
                status = 'completado_con_advertencias'
                message = "Contrato procesado con advertencias"
            
            final_details = {
                'validación': validation,
                'tipo_contrato': contract_data.get('tipo_contrato'),
                'numero_contrato': contract_data.get('numero_contrato'),
                'campos_extraídos': [k for k, v in contract_data.items() if v is not None],
                'requires_review': requires_review,
                'datos_guardados': should_save
            }
            
            update_document_processing_status(
                document_id, 
                status, 
                json.dumps(final_details, ensure_ascii=False),
                tipo_documento='Contrato'
            )
            
            documento_detalle['confianza'] = validation['confidence']
            documento_detalle['estado_final'] = status
            
            # Finalizar registro principal
            log_document_processing_end(
                registro_id, 
                estado='completado',
                confianza=validation['confidence'],
                datos_salida=final_details,
                mensaje_error=None if validation['is_valid'] else "Procesado con advertencias"
            )
            
            logger.info(f"✅ Documento {document_id} procesado completamente")
            logger.info(f"   📋 Tipo contrato: {contract_data.get('tipo_contrato')}")
            logger.info(f"   📊 Confianza: {validation['confidence']:.2f}")
            logger.info(f"   📝 Estado: {status}")
                
        except Exception as e:
            error_msg = str(e)
            logger.error(f"❌ Error procesando contrato {document_id if 'document_id' in locals() else 'DESCONOCIDO'}: {error_msg}")
            logger.error(traceback.format_exc())
            
            documento_detalle['estado'] = 'error'
            documento_detalle['error'] = error_msg
            response['errores'] += 1
            
            # Actualizar estado de error
            if 'document_id' in locals():
                try:
                    update_document_processing_status(
                        document_id, 
                        'error',
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

    # Asignar carpeta (solo si hay éxitos)
    if response['procesados'] > 0 or response['requieren_revision'] > 0:
        # Buscar CUALQUIER documento que haya sido procesado
        last_processed_doc = None
        for detalle in response['detalles']:
            if detalle['estado'] in ['procesado', 'procesado_completo', 'requiere_revision', 'datos_insuficientes']:
                last_processed_doc = detalle['documento_id']
                break
        
        if last_processed_doc:
            cliente_id = get_client_id_by_document(last_processed_doc)
            if cliente_id:
                logger.info(f"👤 Asignando carpeta para documento {last_processed_doc}")
                assign_folder_and_link(cliente_id, last_processed_doc)
    
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
    logger.info(f"⏱️ Tiempo total: {total_time:.2f} segundos")
    
    if response['procesados'] > 0 or response['requieren_revision'] > 0:
        logger.info("🎉 Procesamiento completado con resultados")
    else:
        logger.warning("⚠️ Procesamiento completado SIN contratos exitosos")
    
    return {
        'statusCode': 200,
        'body': json.dumps(response, ensure_ascii=False)
    }
 