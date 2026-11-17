package cl.barriodigital.kafkaadmin.dto;

import java.util.List;

/**
 * QUÉ ES: DTO de salida con el detalle de un consumer group.
 * @param state    estado del grupo (STABLE = con consumidores activos, EMPTY = sin consumidores, etc.)
 * @param totalLag suma del atraso de todas las particiones: mensajes publicados que el grupo aún no procesa
 */
public record GroupDetail(
        String groupId,
        String state,
        String coordinator,
        List<MemberInfo> members,
        List<OffsetLag> offsets,
        long totalLag) {
}
