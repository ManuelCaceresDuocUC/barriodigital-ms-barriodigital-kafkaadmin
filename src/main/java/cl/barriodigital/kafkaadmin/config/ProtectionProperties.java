package cl.barriodigital.kafkaadmin.config;

import java.util.List;

import org.springframework.boot.context.properties.ConfigurationProperties;

/** Nombres (con comodín *) de los tópicos que la API no permite eliminar. */
@ConfigurationProperties(prefix = "barriodigital.kafkaadmin.protection")
public record ProtectionProperties(List<String> topics) {

    public ProtectionProperties {
        topics = topics == null ? List.of() : List.copyOf(topics);
    }
}
