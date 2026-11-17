package cl.barriodigital.kafkaadmin.dto;

/** QUÉ ES: DTO de salida: el nuevo offset que quedó en una partición tras resetear un grupo. */
public record PartitionOffset(int partition, long offset) {
}
