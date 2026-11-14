package cl.barriodigital.kafkaadmin.exception;

public class TopicNotFoundException extends KafkaAdminException {

    public TopicNotFoundException(String name) {
        super("El tópico '" + name + "' no existe");
    }
}
