package cl.barriodigital.kafkaadmin.config;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Cliente de administración de Kafka. Crearlo no abre conexión: el servicio arranca aunque el clúster esté caído.
 *
 * <p>QUÉ ES: crea el bean {@link Admin} (cliente oficial de Kafka para administrar tópicos, grupos y clúster),
 * que usan los tres servicios de este microservicio.
 */
@Configuration
public class KafkaAdminConfig {

    // destroyMethod = "close": al apagar la app se cierra el cliente y sus conexiones.
    @Bean(destroyMethod = "close")
    Admin kafkaAdminClient(KafkaConnectionProperties properties) {
        int timeoutMs = (int) properties.timeout().toMillis();
        Map<String, Object> config = new HashMap<>();
        // Dirección de los brokers (IP privada de ec2-kafka, por la variable KAFKA_BOOTSTRAP_SERVERS).
        config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, properties.bootstrapServers());
        config.put(AdminClientConfig.CLIENT_ID_CONFIG, "barriodigital-kafkaadmin");
        // Tiempos máximos por operación: así una caída de Kafka produce un error rápido y no una espera infinita.
        config.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, timeoutMs);
        config.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, timeoutMs);
        return Admin.create(config);
    }
}
