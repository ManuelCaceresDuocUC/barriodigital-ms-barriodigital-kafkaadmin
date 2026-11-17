package cl.barriodigital.kafkaadmin.exception;

/**
 * Base de las excepciones de dominio del administrador; el handler las traduce a HTTP.
 * QUÉ ES: clase abstracta común. Permite que GlobalExceptionHandler agrupe varias excepciones en un
 * solo método. Hereda de RuntimeException: no obliga a declarar "throws".
 */
public abstract class KafkaAdminException extends RuntimeException {

    protected KafkaAdminException(String message) {
        super(message);
    }

    // Variante que conserva la excepción original (causa) para poder verla en el log.
    protected KafkaAdminException(String message, Throwable cause) {
        super(message, cause);
    }
}
