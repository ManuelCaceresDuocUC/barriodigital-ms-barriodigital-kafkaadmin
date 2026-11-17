package cl.barriodigital.kafkaadmin;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;

/**
 * QUÉ ES: punto de entrada del microservicio administrador de Kafka (kafkaadmin).
 *
 * QUÉ HACE: expone una API REST (solo rol Admin) para administrar el clúster Kafka: tópicos, consumer groups
 * y estado del clúster. Al arrancar, además, crea los tópicos del sistema si faltan (TopicProvisioner).
 * No usa base de datos: todo el estado vive en Kafka. Es el equivalente de rabbitadmin, pero para Kafka.
 */
@SpringBootApplication
// Escanea y registra automáticamente todos los @ConfigurationProperties del paquete (sin @EnableConfigurationProperties).
@ConfigurationPropertiesScan
public class KafkaAdminApplication {

    public static void main(String[] args) {
        SpringApplication.run(KafkaAdminApplication.class, args);
    }
}
