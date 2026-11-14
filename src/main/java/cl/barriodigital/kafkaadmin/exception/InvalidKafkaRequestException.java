package cl.barriodigital.kafkaadmin.exception;

/** La petición es válida como JSON pero Kafka o las reglas del servicio la rechazan (400). */
public class InvalidKafkaRequestException extends KafkaAdminException {

    public InvalidKafkaRequestException(String message) {
        super(message);
    }
}
