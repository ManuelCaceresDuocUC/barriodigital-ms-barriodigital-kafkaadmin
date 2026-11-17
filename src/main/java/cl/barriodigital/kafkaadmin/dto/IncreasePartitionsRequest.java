package cl.barriodigital.kafkaadmin.dto;

import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;

/**
 * Nuevo total de particiones del tópico (Kafka solo permite aumentarlas).
 * QUÉ ES: DTO de entrada de PUT /api/kafka/topics/{name}/partitions. Que el valor sea mayor al actual
 * lo valida TopicService (necesita consultar el tópico).
 */
public record IncreasePartitionsRequest(
        @NotNull(message = "El total de particiones es obligatorio")
        @Min(value = 1, message = "Debe haber al menos 1 partición")
        @Max(value = ValidationRules.MAX_PARTITIONS, message = "No se permiten más de 50 particiones")
        Integer partitions) {
}
