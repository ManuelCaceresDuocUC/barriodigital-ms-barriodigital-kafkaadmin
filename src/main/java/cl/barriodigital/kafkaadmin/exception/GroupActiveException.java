package cl.barriodigital.kafkaadmin.exception;

public class GroupActiveException extends KafkaAdminException {

    public GroupActiveException(String group, String state) {
        super("El consumer group '" + group + "' está " + state
                + ": detén sus consumidores (debe quedar EMPTY) antes de resetear sus offsets");
    }
}
