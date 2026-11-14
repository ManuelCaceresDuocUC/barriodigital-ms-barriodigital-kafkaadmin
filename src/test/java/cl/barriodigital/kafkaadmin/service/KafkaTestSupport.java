package cl.barriodigital.kafkaadmin.service;

import java.time.Duration;

import cl.barriodigital.kafkaadmin.config.KafkaConnectionProperties;
import cl.barriodigital.kafkaadmin.config.ProtectionProperties;
import cl.barriodigital.kafkaadmin.util.KafkaCalls;

import java.util.List;

/** Utilidades compartidas por los tests de servicios. */
final class KafkaTestSupport {

    private KafkaTestSupport() {
    }

    static KafkaCalls calls() {
        return new KafkaCalls(new KafkaConnectionProperties("localhost:9092", Duration.ofSeconds(1)));
    }

    static ProtectedTopics protectedTopics(String... patterns) {
        return new ProtectedTopics(new ProtectionProperties(List.of(patterns)));
    }
}
