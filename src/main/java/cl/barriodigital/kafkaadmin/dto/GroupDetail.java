package cl.barriodigital.kafkaadmin.dto;

import java.util.List;

public record GroupDetail(
        String groupId,
        String state,
        String coordinator,
        List<MemberInfo> members,
        List<OffsetLag> offsets,
        long totalLag) {
}
