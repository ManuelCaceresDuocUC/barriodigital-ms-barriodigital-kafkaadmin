package cl.barriodigital.kafkaadmin.exception;

/** QUÉ ES: excepción "el consumer group no existe". Se traduce a HTTP 404. */
public class GroupNotFoundException extends KafkaAdminException {

    public GroupNotFoundException(String name) {
        super("El consumer group '" + name + "' no existe");
    }
}
