package cl.barriodigital.kafkaadmin.config;

import java.time.Duration;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.util.Assert;

/**
 * Conexión al clúster: brokers (IP privada de ec2-kafka) y tiempo máximo de espera por operación.
 * QUÉ ES: record enlazado al bloque barriodigital.kafkaadmin.connection del yaml, con validación.
 */
@ConfigurationProperties(prefix = "barriodigital.kafkaadmin.connection")
public record KafkaConnectionProperties(String bootstrapServers, Duration timeout) {

    // Constructor compacto: valida al arrancar y asigna un valor por defecto (10 s) al timeout.
    public KafkaConnectionProperties {
        Assert.hasText(bootstrapServers, "bootstrap-servers es obligatorio (KAFKA_BOOTSTRAP_SERVERS)");
        timeout = timeout == null ? Duration.ofSeconds(10) : timeout;
        Assert.isTrue(!timeout.isNegative() && !timeout.isZero(), "timeout debe ser mayor que cero");
    }
}
