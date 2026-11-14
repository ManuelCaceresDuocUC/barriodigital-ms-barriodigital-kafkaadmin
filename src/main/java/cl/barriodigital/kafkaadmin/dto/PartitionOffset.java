package cl.barriodigital.kafkaadmin.dto;

public record PartitionOffset(int partition, long offset) {
}
