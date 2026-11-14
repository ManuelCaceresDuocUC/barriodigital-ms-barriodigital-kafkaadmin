package cl.barriodigital.kafkaadmin.exception;

public class ProtectedTopicException extends KafkaAdminException {

    public ProtectedTopicException(String name) {
        super("El tópico '" + name + "' pertenece al sistema y no se puede eliminar");
    }
}
