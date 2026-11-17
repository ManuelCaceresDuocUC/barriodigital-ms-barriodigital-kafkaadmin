package cl.barriodigital.kafkaadmin.dto;

/**
 * Límites compartidos por las validaciones de las peticiones.
 * QUÉ ES: clase de constantes (constructor privado: no se instancia) para usar los mismos números en todos los DTO.
 */
public final class ValidationRules {

    // Largo máximo del nombre de un tópico en Kafka.
    public static final int TOPIC_NAME_MAX = 249;
    // Caracteres que Kafka admite en un nombre de tópico.
    public static final String TOPIC_NAME_REGEX = "[a-zA-Z0-9._-]+";
    /** El clúster tiene 3 brokers: no tiene sentido pedir más réplicas. */
    public static final int MAX_REPLICATION = 3;
    public static final int MAX_PARTITIONS = 50;

    private ValidationRules() {
    }
}
