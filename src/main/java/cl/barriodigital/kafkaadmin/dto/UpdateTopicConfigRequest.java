package cl.barriodigital.kafkaadmin.dto;

import java.util.Map;

import jakarta.validation.constraints.NotEmpty;

/** Configuraciones a fijar en un tópico existente (por ejemplo {@code retention.ms}). */
public record UpdateTopicConfigRequest(
        @NotEmpty(message = "Debes indicar al menos una configuración")
        Map<String, String> configs) {
}
