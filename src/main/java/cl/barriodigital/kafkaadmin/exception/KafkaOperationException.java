package cl.barriodigital.kafkaadmin.exception;

/**
 * Kafka no respondió o falló: es un problema aguas arriba, no del cliente (502).
 * QUÉ ES: excepción para fallas de infraestructura (clúster caído, tiempo agotado, error inesperado).
 */
public class KafkaOperationException extends KafkaAdminException {

    public KafkaOperationException(String message, Throwable cause) {
        super(message, cause);
    }
}
