package cl.barriodigital.kafkaadmin.dto;

/**
 * Resultado de aprovisionar un tópico del sistema: CREATED si se creó, EXISTS si ya estaba.
 * QUÉ ES: DTO de salida de POST /api/kafka/topics/provision (y lo que registra TopicProvisioner al arrancar).
 */
public record ProvisionResult(String topic, String status) {

    public static final String CREATED = "CREATED";
    public static final String EXISTS = "EXISTS";
}
