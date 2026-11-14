package cl.barriodigital.kafkaadmin.dto;

import java.util.List;

public record TopicDetail(
        String name,
        int partitionCount,
        int replicationFactor,
        boolean protectedTopic,
        List<PartitionDetail> partitions,
        List<ConfigDetail> configs) {
}
