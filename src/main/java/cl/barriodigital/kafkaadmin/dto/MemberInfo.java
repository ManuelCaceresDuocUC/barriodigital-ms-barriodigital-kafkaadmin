package cl.barriodigital.kafkaadmin.dto;

import java.util.List;

/** Un consumidor del grupo y las particiones que tiene asignadas (formato tópico-partición). */
public record MemberInfo(String memberId, String clientId, String host, List<String> assignedPartitions) {
}
