package cl.barriodigital.kafkaadmin.dto;

/** QUÉ ES: DTO de salida de la lista de tópicos (una fila por tópico). */
public record TopicSummary(String name, int partitions, int replicationFactor, boolean protectedTopic) {
}
