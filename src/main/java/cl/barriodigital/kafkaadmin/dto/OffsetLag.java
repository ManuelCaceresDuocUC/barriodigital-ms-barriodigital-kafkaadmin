package cl.barriodigital.kafkaadmin.dto;

/** Posición confirmada del grupo en una partición, fin del log y atraso (lag) entre ambos. */
public record OffsetLag(String topic, int partition, long currentOffset, long endOffset, long lag) {
}
