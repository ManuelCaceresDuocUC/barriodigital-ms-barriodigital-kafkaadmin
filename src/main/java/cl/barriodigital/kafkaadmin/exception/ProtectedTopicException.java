package cl.barriodigital.kafkaadmin.exception;

/**
 * QUÉ ES: excepción "el tópico es del sistema y no se puede eliminar" (requests.events, audit.timeline).
 * Se traduce a HTTP 409. Evita que un clic accidental rompa el flujo de reportería y auditoría.
 */
public class ProtectedTopicException extends KafkaAdminException {

    public ProtectedTopicException(String name) {
        super("El tópico '" + name + "' pertenece al sistema y no se puede eliminar");
    }
}
