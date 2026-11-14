package cl.barriodigital.kafkaadmin.exception;

public class GroupNotFoundException extends KafkaAdminException {

    public GroupNotFoundException(String name) {
        super("El consumer group '" + name + "' no existe");
    }
}
