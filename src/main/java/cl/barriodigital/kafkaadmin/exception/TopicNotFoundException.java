package cl.barriodigital.kafkaadmin.exception;

/** QUÉ ES: excepción "el tópico no existe". Se traduce a HTTP 404. */
public class TopicNotFoundException extends KafkaAdminException {

    public TopicNotFoundException(String name) {
        super("El tópico '" + name + "' no existe");
    }
}
