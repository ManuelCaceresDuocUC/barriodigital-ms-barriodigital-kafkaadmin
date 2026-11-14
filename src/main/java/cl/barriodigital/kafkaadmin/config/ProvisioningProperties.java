package cl.barriodigital.kafkaadmin.config;

import java.util.List;
import java.util.Map;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.util.Assert;

/** Tópicos del sistema que se crean al arrancar si no existen (idempotente). */
@ConfigurationProperties(prefix = "barriodigital.kafkaadmin.provisioning")
public record ProvisioningProperties(boolean enabled, List<TopicDefinition> topics) {

    public ProvisioningProperties {
        topics = topics == null ? List.of() : List.copyOf(topics);
    }

    public record TopicDefinition(String name, int partitions, int replicationFactor, Map<String, String> configs) {

        public TopicDefinition {
            Assert.hasText(name, "El tópico a aprovisionar necesita un nombre");
            Assert.isTrue(partitions >= 1, "partitions debe ser al menos 1 en el tópico " + name);
            Assert.isTrue(replicationFactor >= 1, "replication-factor debe ser al menos 1 en el tópico " + name);
            configs = configs == null ? Map.of() : Map.copyOf(configs);
        }
    }
}
