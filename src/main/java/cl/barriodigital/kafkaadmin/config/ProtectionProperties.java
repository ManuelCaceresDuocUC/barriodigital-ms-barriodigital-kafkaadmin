package cl.barriodigital.kafkaadmin.config;

import java.util.List;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Nombres (con comodín *) de los tópicos que la API no permite eliminar.
 * QUÉ ES: record enlazado al bloque barriodigital.kafkaadmin.protection del yaml. Lo usa ProtectedTopics.
 */
@ConfigurationProperties(prefix = "barriodigital.kafkaadmin.protection")
public record ProtectionProperties(List<String> topics) {

    public ProtectionProperties {
        // Lista inmutable; si no se configura nada, ningún tópico queda protegido.
        topics = topics == null ? List.of() : List.copyOf(topics);
    }
}
