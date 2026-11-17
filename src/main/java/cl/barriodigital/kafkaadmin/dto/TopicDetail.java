package cl.barriodigital.kafkaadmin.dto;

import java.util.List;

/**
 * QUÉ ES: DTO de salida de GET /api/kafka/topics/{name}: resumen, detalle por partición y configuración.
 * @param protectedTopic true si es un tópico del sistema (no se puede eliminar)
 */
public record TopicDetail(
        String name,
        int partitionCount,
        int replicationFactor,
        boolean protectedTopic,
        List<PartitionDetail> partitions,
        List<ConfigDetail> configs) {
}
