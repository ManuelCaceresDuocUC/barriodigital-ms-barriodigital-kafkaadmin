package cl.barriodigital.kafkaadmin.dto;

import java.util.List;

/**
 * Líder, réplicas e ISR (réplicas en sincronía) de una partición, como ids de broker.
 * CONCEPTO: el líder atiende lecturas y escrituras; las demás réplicas lo copian. Si el ISR es más corto que
 * la lista de réplicas, alguna réplica va atrasada o su broker está caído.
 */
public record PartitionDetail(int partition, Integer leader, List<Integer> replicas, List<Integer> isr) {
}
