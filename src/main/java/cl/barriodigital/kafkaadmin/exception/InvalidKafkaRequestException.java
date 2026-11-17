package cl.barriodigital.kafkaadmin.exception;

/**
 * La petición es válida como JSON pero Kafka o las reglas del servicio la rechazan (400).
 * QUÉ ES: excepción para errores del cliente que solo se detectan al aplicar reglas (por ejemplo reducir particiones).
 */
public class InvalidKafkaRequestException extends KafkaAdminException {

    public InvalidKafkaRequestException(String message) {
        super(message);
    }
}
