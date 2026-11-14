package cl.barriodigital.kafkaadmin.dto;

public record TopicSummary(String name, int partitions, int replicationFactor, boolean protectedTopic) {
}
