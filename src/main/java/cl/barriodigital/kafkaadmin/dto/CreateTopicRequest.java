package cl.barriodigital.kafkaadmin.dto;

import java.util.Map;

import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Size;

/** Datos para crear un tópico; {@code configs} es opcional (retención, cleanup policy, etc.). */
public record CreateTopicRequest(
        @NotBlank(message = "El nombre del tópico es obligatorio")
        @Size(max = ValidationRules.TOPIC_NAME_MAX, message = "El nombre no puede superar los 249 caracteres")
        @Pattern(regexp = ValidationRules.TOPIC_NAME_REGEX,
                message = "El nombre solo admite letras, números, punto, guion y guion bajo")
        String name,

        @NotNull(message = "Las particiones son obligatorias")
        @Min(value = 1, message = "Debe haber al menos 1 partición")
        @Max(value = ValidationRules.MAX_PARTITIONS, message = "No se permiten más de 50 particiones")
        Integer partitions,

        @NotNull(message = "El factor de replicación es obligatorio")
        @Min(value = 1, message = "El factor de replicación debe ser al menos 1")
        @Max(value = ValidationRules.MAX_REPLICATION, message = "El clúster tiene 3 brokers: máximo 3 réplicas")
        Integer replicationFactor,

        Map<String, String> configs) {
}
