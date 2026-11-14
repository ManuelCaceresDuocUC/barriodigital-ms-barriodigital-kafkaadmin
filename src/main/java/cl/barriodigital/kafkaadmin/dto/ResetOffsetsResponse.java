package cl.barriodigital.kafkaadmin.dto;

import java.util.List;

public record ResetOffsetsResponse(String groupId, String topic, ResetStrategy strategy, List<PartitionOffset> offsets) {
}
