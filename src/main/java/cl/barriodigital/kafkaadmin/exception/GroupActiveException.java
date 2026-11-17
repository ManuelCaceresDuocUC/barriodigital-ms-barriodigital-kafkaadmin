package cl.barriodigital.kafkaadmin.exception;

/**
 * QUÉ ES: excepción "no se pueden resetear los offsets porque el grupo está activo". Se traduce a HTTP 409.
 * Kafka exige que el grupo esté EMPTY (sin consumidores conectados) para mover sus offsets.
 */
public class GroupActiveException extends KafkaAdminException {

    public GroupActiveException(String group, String state) {
        super("El consumer group '" + group + "' está " + state
                + ": detén sus consumidores (debe quedar EMPTY) antes de resetear sus offsets");
    }
}
