package cl.barriodigital.kafkaadmin.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.PositiveOrZero;

/**
 * Resetea los offsets de un grupo sobre un tópico. {@code offset} es obligatorio con la estrategia OFFSET;
 * {@code partition} es opcional (si falta, se aplica a todas las particiones del tópico).
 *
 * <p>QUÉ ES: DTO de entrada de POST .../offsets/reset. Sirve, por ejemplo, para que un consumidor vuelva a
 * leer todo desde el principio (EARLIEST) y recalcule métricas.
 */
public record ResetOffsetsRequest(
        @NotBlank(message = "El tópico es obligatorio")
        String topic,

        @NotNull(message = "La estrategia es obligatoria (EARLIEST, LATEST u OFFSET)")
        ResetStrategy strategy,

        @PositiveOrZero(message = "El offset no puede ser negativo")
        Long offset,

        @PositiveOrZero(message = "La partición no puede ser negativa")
        Integer partition) {
}
