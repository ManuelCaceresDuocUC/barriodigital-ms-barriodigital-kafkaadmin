package cl.barriodigital.kafkaadmin.service;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

import cl.barriodigital.kafkaadmin.config.ProvisioningProperties;
import cl.barriodigital.kafkaadmin.config.ProvisioningProperties.TopicDefinition;
import cl.barriodigital.kafkaadmin.dto.ProvisionResult;
import cl.barriodigital.kafkaadmin.exception.TopicAlreadyExistsException;
import cl.barriodigital.kafkaadmin.util.KafkaCalls;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * QUÉ ES: ApplicationRunner, es decir, código que Spring ejecuta una vez al terminar de arrancar.
 * QUÉ HACE: crea los tópicos del sistema (requests.events y audit.timeline) si faltan. Es idempotente: uno que ya existe no se toca.
 * Al arrancar nunca tumba el servicio: si Kafka no está disponible lo registra y se puede reintentar por la API.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class TopicProvisioner implements ApplicationRunner {

    // Cliente Admin de Kafka.
    private final Admin admin;
    private final KafkaCalls kafka;
    private final ProvisioningProperties properties;

    // Se ejecuta automáticamente al iniciar el servicio.
    @Override
    public void run(ApplicationArguments args) {
        if (!properties.enabled()) {
            log.info("Aprovisionamiento de tópicos deshabilitado");
            return;
        }
        try {
            provision().forEach(result -> log.info("Tópico '{}': {}", result.topic(), result.status()));
        } catch (RuntimeException ex) {
            log.warn("No se pudo aprovisionar los tópicos al arrancar (se puede reintentar con POST /api/kafka/topics/provision): {}",
                    ex.getMessage());
        }
    }

    // Aprovisiona todos los tópicos definidos en el yaml. También lo usa POST /api/kafka/topics/provision.
    public List<ProvisionResult> provision() {
        List<ProvisionResult> results = new ArrayList<>();
        for (TopicDefinition definition : properties.topics()) {
            results.add(provisionOne(definition));
        }
        return results;
    }

    // Crea un tópico; si Kafka dice que ya existe, lo informa como EXISTS en vez de fallar.
    private ProvisionResult provisionOne(TopicDefinition definition) {
        NewTopic topic = new NewTopic(definition.name(), definition.partitions(),
                (short) definition.replicationFactor());
        if (!definition.configs().isEmpty()) {
            topic.configs(Map.copyOf(definition.configs()));
        }
        try {
            kafka.await(admin.createTopics(List.of(topic)).all(), "crear el tópico", definition.name());
            return new ProvisionResult(definition.name(), ProvisionResult.CREATED);
        } catch (TopicAlreadyExistsException ex) {
            return new ProvisionResult(definition.name(), ProvisionResult.EXISTS);
        }
    }
}
