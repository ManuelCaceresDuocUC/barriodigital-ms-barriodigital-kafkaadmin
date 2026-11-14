package cl.barriodigital.kafkaadmin.exception;

/** Kafka no respondió o falló: es un problema aguas arriba, no del cliente (502). */
public class KafkaOperationException extends KafkaAdminException {

    public KafkaOperationException(String message, Throwable cause) {
        super(message, cause);
    }
}
