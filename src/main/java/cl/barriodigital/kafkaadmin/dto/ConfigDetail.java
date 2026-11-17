package cl.barriodigital.kafkaadmin.dto;

/**
 * Una configuración del tópico y de dónde viene su valor (DYNAMIC_TOPIC_CONFIG si se fijó en el tópico).
 * QUÉ ES: DTO de salida; readOnly indica si la configuración se puede modificar o no.
 */
public record ConfigDetail(String name, String value, String source, boolean readOnly) {
}
