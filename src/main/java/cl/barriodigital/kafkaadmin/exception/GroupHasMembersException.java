package cl.barriodigital.kafkaadmin.exception;

public class GroupHasMembersException extends KafkaAdminException {

    public GroupHasMembersException(String name) {
        super("El consumer group '" + name + "' tiene miembros activos: detén sus consumidores antes de eliminarlo");
    }
}
