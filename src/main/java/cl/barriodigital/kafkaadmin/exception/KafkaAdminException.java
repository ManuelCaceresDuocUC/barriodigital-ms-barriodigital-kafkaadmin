package cl.barriodigital.kafkaadmin.exception;

/** Base de las excepciones de dominio del administrador; el handler las traduce a HTTP. */
public abstract class KafkaAdminException extends RuntimeException {

    protected KafkaAdminException(String message) {
        super(message);
    }

    protected KafkaAdminException(String message, Throwable cause) {
        super(message, cause);
    }
}
