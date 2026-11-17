package cl.barriodigital.kafkaadmin.dto;

import java.util.List;

/** QUÉ ES: DTO de salida del reseteo de offsets: grupo, tópico, estrategia usada y nuevo offset por partición. */
public record ResetOffsetsResponse(String groupId, String topic, ResetStrategy strategy, List<PartitionOffset> offsets) {
}
