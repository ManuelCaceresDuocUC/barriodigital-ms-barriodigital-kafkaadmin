package cl.barriodigital.kafkaadmin.exception;

/** QUÉ ES: excepción "ya existe un tópico con ese nombre". Se traduce a HTTP 409. */
public class TopicAlreadyExistsException extends KafkaAdminException {

    public TopicAlreadyExistsException(String name) {
        super("El tópico '" + name + "' ya existe");
    }
}
