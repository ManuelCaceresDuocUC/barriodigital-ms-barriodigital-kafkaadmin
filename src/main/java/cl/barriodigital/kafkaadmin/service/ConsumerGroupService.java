package cl.barriodigital.kafkaadmin.service;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.ConsumerGroupListing;
import org.apache.kafka.clients.admin.ListOffsetsResult.ListOffsetsResultInfo;
import org.apache.kafka.clients.admin.MemberDescription;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.springframework.stereotype.Service;

import cl.barriodigital.kafkaadmin.dto.GroupDetail;
import cl.barriodigital.kafkaadmin.dto.GroupSummary;
import cl.barriodigital.kafkaadmin.dto.MemberInfo;
import cl.barriodigital.kafkaadmin.dto.OffsetLag;
import cl.barriodigital.kafkaadmin.dto.PartitionOffset;
import cl.barriodigital.kafkaadmin.dto.ResetOffsetsRequest;
import cl.barriodigital.kafkaadmin.dto.ResetOffsetsResponse;
import cl.barriodigital.kafkaadmin.dto.ResetStrategy;
import cl.barriodigital.kafkaadmin.exception.GroupActiveException;
import cl.barriodigital.kafkaadmin.exception.InvalidKafkaRequestException;
import cl.barriodigital.kafkaadmin.util.KafkaCalls;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/** Consumer groups: estado, miembros, offsets con lag, reseteo de offsets y eliminación. */
@Slf4j
@Service
@RequiredArgsConstructor
public class ConsumerGroupService {

    private static final String EMPTY = "EMPTY";

    private final Admin admin;
    private final KafkaCalls kafka;

    public List<GroupSummary> list() {
        Collection<ConsumerGroupListing> groups =
                kafka.await(admin.listConsumerGroups().all(), "listar los consumer groups", "");
        return groups.stream()
                .map(group -> new GroupSummary(group.groupId(), stateName(group.groupState().orElse(null))))
                .sorted(Comparator.comparing(GroupSummary::groupId))
                .toList();
    }

    public GroupDetail get(String groupId) {
        ConsumerGroupDescription description = describe(groupId);

        List<MemberInfo> members = description.members().stream()
                .sorted(Comparator.comparing(MemberDescription::consumerId))
                .map(member -> new MemberInfo(member.consumerId(), member.clientId(), member.host(),
                        member.assignment().topicPartitions().stream()
                                .map(tp -> tp.topic() + "-" + tp.partition()).sorted().toList()))
                .toList();

        Map<TopicPartition, OffsetAndMetadata> committed = committedOffsets(groupId);
        Map<TopicPartition, ListOffsetsResultInfo> ends = endOffsets(committed.keySet(), groupId);
        List<OffsetLag> offsets = committed.entrySet().stream()
                .sorted(Comparator.comparing((Map.Entry<TopicPartition, OffsetAndMetadata> e) -> e.getKey().topic())
                        .thenComparingInt(e -> e.getKey().partition()))
                .map(entry -> {
                    long current = entry.getValue().offset();
                    ListOffsetsResultInfo end = ends.get(entry.getKey());
                    long endOffset = end == null ? current : end.offset();
                    return new OffsetLag(entry.getKey().topic(), entry.getKey().partition(), current, endOffset,
                            Math.max(0, endOffset - current));
                })
                .toList();

        String coordinator = description.coordinator() == null ? null
                : description.coordinator().host() + ":" + description.coordinator().port();
        long totalLag = offsets.stream().mapToLong(OffsetLag::lag).sum();
        return new GroupDetail(groupId, stateName(description.groupState()), coordinator, members, offsets, totalLag);
    }

    /**
     * Mueve los offsets del grupo sobre un tópico. Kafka solo lo permite con el grupo vacío (sin consumidores
     * conectados), por eso se valida antes y se responde 409 con instrucciones en vez de un error genérico.
     */
    public ResetOffsetsResponse resetOffsets(String groupId, ResetOffsetsRequest request) {
        ConsumerGroupDescription description = describe(groupId);
        String state = stateName(description.groupState());
        if (!EMPTY.equals(state)) {
            throw new GroupActiveException(groupId, state);
        }
        if (request.strategy() == ResetStrategy.OFFSET && request.offset() == null) {
            throw new InvalidKafkaRequestException("La estrategia OFFSET requiere indicar el offset");
        }

        TopicDescription topic = kafka.await(
                admin.describeTopics(List.of(request.topic())).allTopicNames(), "describir el tópico",
                request.topic()).get(request.topic());
        List<TopicPartition> partitions = topic.partitions().stream()
                .map(info -> new TopicPartition(request.topic(), info.partition()))
                .filter(tp -> request.partition() == null || tp.partition() == request.partition())
                .toList();
        if (partitions.isEmpty()) {
            throw new InvalidKafkaRequestException("El tópico '" + request.topic() + "' no tiene la partición "
                    + request.partition());
        }

        Map<TopicPartition, OffsetAndMetadata> target = new HashMap<>();
        switch (request.strategy()) {
            case OFFSET -> partitions.forEach(tp -> target.put(tp, new OffsetAndMetadata(request.offset())));
            case EARLIEST, LATEST -> {
                OffsetSpec spec = request.strategy() == ResetStrategy.EARLIEST
                        ? OffsetSpec.earliest() : OffsetSpec.latest();
                Map<TopicPartition, OffsetSpec> query = partitions.stream()
                        .collect(Collectors.toMap(tp -> tp, tp -> spec));
                kafka.await(admin.listOffsets(query).all(), "consultar los offsets", request.topic())
                        .forEach((tp, info) -> target.put(tp, new OffsetAndMetadata(info.offset())));
            }
        }

        kafka.await(admin.alterConsumerGroupOffsets(groupId, target).all(), "resetear los offsets", groupId);
        log.info("Offsets del grupo '{}' en '{}' reseteados ({})", groupId, request.topic(), request.strategy());

        List<PartitionOffset> result = new ArrayList<>();
        target.forEach((tp, offset) -> result.add(new PartitionOffset(tp.partition(), offset.offset())));
        result.sort(Comparator.comparingInt(PartitionOffset::partition));
        return new ResetOffsetsResponse(groupId, request.topic(), request.strategy(), result);
    }

    public void delete(String groupId) {
        kafka.await(admin.deleteConsumerGroups(List.of(groupId)).all(), "eliminar el consumer group", groupId);
        log.info("Consumer group '{}' eliminado", groupId);
    }

    private ConsumerGroupDescription describe(String groupId) {
        Map<String, ConsumerGroupDescription> result = kafka.await(
                admin.describeConsumerGroups(List.of(groupId)).all(), "describir el consumer group", groupId);
        ConsumerGroupDescription description = result.get(groupId);
        // Un grupo inexistente se describe como DEAD en vez de fallar.
        if (description == null || "DEAD".equals(stateName(description.groupState()))) {
            throw new cl.barriodigital.kafkaadmin.exception.GroupNotFoundException(groupId);
        }
        return description;
    }

    private Map<TopicPartition, OffsetAndMetadata> committedOffsets(String groupId) {
        Map<TopicPartition, OffsetAndMetadata> committed = kafka.await(
                admin.listConsumerGroupOffsets(groupId).partitionsToOffsetAndMetadata(),
                "leer los offsets del grupo", groupId);
        // Una partición sin offset confirmado llega con valor nulo.
        return committed.entrySet().stream()
                .filter(entry -> entry.getValue() != null)
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    private Map<TopicPartition, ListOffsetsResultInfo> endOffsets(Set<TopicPartition> partitions, String groupId) {
        if (partitions.isEmpty()) {
            return Map.of();
        }
        Map<TopicPartition, OffsetSpec> query = partitions.stream()
                .collect(Collectors.toMap(tp -> tp, tp -> OffsetSpec.latest()));
        return kafka.await(admin.listOffsets(query).all(), "consultar el fin de los logs", groupId);
    }

    private static String stateName(org.apache.kafka.common.GroupState state) {
        return state == null ? "UNKNOWN" : state.name();
    }
}
