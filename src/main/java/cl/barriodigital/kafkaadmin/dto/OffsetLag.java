package cl.barriodigital.kafkaadmin.dto;

/**
 * Posición confirmada del grupo en una partición, fin del log y atraso (lag) entre ambos.
 * EJEMPLO: currentOffset=10, endOffset=12 -> lag=2 (faltan 2 mensajes por procesar). Lag 0 = al día.
 */
public record OffsetLag(String topic, int partition, long currentOffset, long endOffset, long lag) {
}
