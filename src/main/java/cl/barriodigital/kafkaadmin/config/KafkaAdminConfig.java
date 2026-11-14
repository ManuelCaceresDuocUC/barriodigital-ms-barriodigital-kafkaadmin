package cl.barriodigital.kafkaadmin.config;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/** Cliente de administración de Kafka. Crearlo no abre conexión: el servicio arranca aunque el clúster esté caído. */
@Configuration
public class KafkaAdminConfig {

    @Bean(destroyMethod = "close")
    Admin kafkaAdminClient(KafkaConnectionProperties properties) {
        int timeoutMs = (int) properties.timeout().toMillis();
        Map<String, Object> config = new HashMap<>();
        config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, properties.bootstrapServers());
        config.put(AdminClientConfig.CLIENT_ID_CONFIG, "barriodigital-kafkaadmin");
        config.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, timeoutMs);
        config.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, timeoutMs);
        return Admin.create(config);
    }
}
