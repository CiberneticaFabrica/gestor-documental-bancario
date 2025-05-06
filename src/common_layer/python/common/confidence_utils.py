# common/confidence_utils.py
import os
import logging
from common.db_connector import update_analysis_record, log_document_processing_start, log_document_processing_end

logger = logging.getLogger()
logger.setLevel(os.environ.get('LOG_LEVEL', 'INFO'))

# Umbral de confianza por defecto (configurable mediante variable de entorno)
DEFAULT_CONFIDENCE_THRESHOLD = float(os.environ.get('CONFIDENCE_THRESHOLD', 0.75))

def mark_for_manual_review(document_id, analysis_id, confidence, 
                          document_type=None, validation_info=None, 
                          extracted_data=None):
    """
    Marca un documento para revisión manual de manera estandarizada
    
    Args:
        document_id (str): ID del documento
        analysis_id (str): ID del análisis
        confidence (float): Valor de confianza
        document_type (str, optional): Tipo de documento
        validation_info (dict, optional): Información de validación
        extracted_data (dict, optional): Datos extraídos para conservar
        
    Returns:
        bool: True si se marcó correctamente, False en caso contrario
    """
    try:
        # Registrar en tabla de procesamiento
        registro_id = log_document_processing_start(
            document_id, 
            'marcado_revision_manual',
            datos_entrada={
                "confidence": confidence, 
                "document_type": document_type,
                "validation_info": validation_info
            }
        )
        
        # Actualizar registro de análisis
        update_success = update_analysis_record(
            document_id=document_id,
            estado_analisis='requiere_revision',
            confianza_clasificacion=confidence,
            requiere_verificacion=True,
            verificado=False,
            mensaje_error=None if validation_info is None 
                          else f"Requiere verificación: {json.dumps(validation_info)}"
        )
        
        # Finalizar registro de procesamiento
        log_document_processing_end(
            registro_id, 
            estado='completado' if update_success else 'error',
            datos_salida=extracted_data,
            mensaje_error=None if update_success else "Error al actualizar registro de análisis"
        )
        
        logger.info(f"Documento {document_id} marcado para revisión manual (confianza: {confidence:.2f})")
        return update_success
        
    except Exception as e:
        logger.error(f"Error al marcar documento {document_id} para revisión: {str(e)}")
        return False
    
def evaluate_confidence(confidence, document_type=None, validation_results=None):
    """
    Evalúa si un documento debe marcarse para revisión manual basado en su confianza
    
    Args:
        confidence (float): Valor de confianza (0-1)
        document_type (str, optional): Tipo de documento
        validation_results (dict, optional): Resultados de validación
    
    Returns:
        bool: True si requiere revisión manual, False en caso contrario
    """
    # Primero obtener umbral específico según tipo de documento
    threshold = get_confidence_threshold(document_type)
    
    # Verificar confianza contra umbral
    requires_review = confidence < threshold
    
    # Factores adicionales que pueden requerir revisión
    if validation_results:
        # Errores críticos siempre requieren revisión
        if validation_results.get('errors') and len(validation_results.get('errors', [])) > 0:
            requires_review = True
            logger.info(f"Se requiere revisión manual debido a errores críticos.")
        
        # Muchas advertencias también pueden requerir revisión
        if validation_results.get('warnings') and len(validation_results.get('warnings', [])) > 3:
            requires_review = True
            logger.info(f"Se requiere revisión manual debido a múltiples advertencias.")
    
    logger.info(f"Evaluación de confianza: valor={confidence:.2f}, umbral={threshold:.2f}, " +
                f"requiere_revisión={requires_review}")
    
    return requires_review

def get_confidence_threshold(document_type=None):
    """
    Obtiene el umbral de confianza para un tipo de documento.
    Actualmente usa un valor global pero podría expandirse para valores específicos.
    
    Args:
        document_type (str, optional): Tipo de documento
    
    Returns:
        float: Umbral de confianza (0-1)
    """
    # En una implementación más avanzada, aquí podrías consultar la BD
    # para obtener umbrales específicos por tipo de documento o cliente
    
    # Por ahora, usamos un mapa simple de tipos de documento a umbrales
    type_thresholds = {
        'dni': 0.80,
        'pasaporte': 0.80,
        'contrato': 0.75,
        'extracto_bancario': 0.70,
        'nomina': 0.75,
        'factura': 0.70,
        'impuesto': 0.75
    }
    
    # Si tenemos un umbral específico, usarlo
    if document_type and document_type.lower() in type_thresholds:
        return type_thresholds[document_type.lower()]
    
    # Devolver umbral por defecto
    return DEFAULT_CONFIDENCE_THRESHOLD