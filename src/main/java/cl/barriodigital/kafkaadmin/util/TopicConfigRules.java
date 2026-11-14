package cl.barriodigital.kafkaadmin.util;

import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import cl.barriodigital.kafkaadmin.exception.InvalidKafkaRequestException;

/** Configuraciones de tópico que la API permite crear o modificar (las demás se rechazan con 400). */
public final class TopicConfigRules {

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

    public static void validate(Map<String, String> configs) {
        if (configs == null) {
            return;
        }
        configs.forEach((key, value) -> {
            if (!ALLOWED.contains(key)) {
                throw new InvalidKafkaRequestException("La configuración '" + key
                        + "' no está permitida. Permitidas: " + String.join(", ", new TreeSet<>(ALLOWED)));
            }
            if (value == null || value.isBlank()) {
                throw new InvalidKafkaRequestException("La configuración '" + key + "' necesita un valor");
            }
        });
    }
}
