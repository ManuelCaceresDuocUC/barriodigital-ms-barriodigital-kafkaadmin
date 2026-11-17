package cl.barriodigital.kafkaadmin.exception;

/** QUÉ ES: excepción "no se puede eliminar un grupo con consumidores activos". Se traduce a HTTP 409. */
public class GroupHasMembersException extends KafkaAdminException {

    public GroupHasMembersException(String name) {
        super("El consumer group '" + name + "' tiene miembros activos: detén sus consumidores antes de eliminarlo");
    }
}
