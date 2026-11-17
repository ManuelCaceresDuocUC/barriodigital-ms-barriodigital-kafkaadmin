package cl.barriodigital.kafkaadmin.util;

import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import cl.barriodigital.kafkaadmin.exception.InvalidKafkaRequestException;

/**
 * Configuraciones de tópico que la API permite crear o modificar (las demás se rechazan con 400).
 * QUÉ ES: lista blanca. Evita que se cambien por error configuraciones delicadas del tópico.
 */
public final class TopicConfigRules {

    // Claves permitidas y su significado:
    //  cleanup.policy: delete (borrar por antigüedad) o compact (conservar el último valor por clave)
    //  retention.ms / retention.bytes: cuánto tiempo / cuántos bytes se conservan los mensajes
    //  segment.ms / segment.bytes: cuándo se cierra un archivo (segmento) del log
    //  max.message.bytes: tamaño máximo de un mensaje
    //  min.insync.replicas: réplicas mínimas sincronizadas para aceptar una escritura con acks=all
    //  compression.type: compresión de los mensajes
    //  delete.retention.ms / min.compaction.lag.ms: ajustes de la compactación
    public static final Set<String> ALLOWED = Set.of(
            "cleanup.policy",
            "retention.ms",
            "retention.bytes",
            "segment.ms",
            "segment.bytes",
            "max.message.bytes",
            "min.insync.replicas",
            "compression.type",
            "delete.retention.ms",
            "min.compaction.lag.ms");

    private TopicConfigRules() {
    }

    // Lanza InvalidKafkaRequestException (400) si hay una clave no permitida o un valor vacío.
    public static void validate(Map<String, String> configs) {
        if (configs == null) {
            return;
        }
        configs.forEach((key, value) -> {
            if (!ALLOWED.contains(key)) {
                // TreeSet ordena las claves permitidas alfabéticamente para que el mensaje sea estable.
                throw new InvalidKafkaRequestException("La configuración '" + key
                        + "' no está permitida. Permitidas: " + String.join(", ", new TreeSet<>(ALLOWED)));
            }
            if (value == null || value.isBlank()) {
                throw new InvalidKafkaRequestException("La configuración '" + key + "' necesita un valor");
            }
        });
    }
}
