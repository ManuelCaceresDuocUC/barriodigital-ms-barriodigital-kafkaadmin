package cl.barriodigital.kafkaadmin.dto;

import java.util.List;

/**
 * Un consumidor del grupo y las particiones que tiene asignadas (formato tópico-partición).
 * QUÉ ES: DTO de salida; un miembro = una instancia de un microservicio consumidor (report o audit).
 */
public record MemberInfo(String memberId, String clientId, String host, List<String> assignedPartitions) {
}
