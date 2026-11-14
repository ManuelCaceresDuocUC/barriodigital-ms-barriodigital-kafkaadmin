package cl.barriodigital.kafkaadmin.dto;

import java.util.List;

/** Líder, réplicas e ISR (réplicas en sincronía) de una partición, como ids de broker. */
public record PartitionDetail(int partition, Integer leader, List<Integer> replicas, List<Integer> isr) {
}
