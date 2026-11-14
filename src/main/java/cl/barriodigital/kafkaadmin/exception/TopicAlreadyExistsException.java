package cl.barriodigital.kafkaadmin.exception;

public class TopicAlreadyExistsException extends KafkaAdminException {

    public TopicAlreadyExistsException(String name) {
        super("El tópico '" + name + "' ya existe");
    }
}
