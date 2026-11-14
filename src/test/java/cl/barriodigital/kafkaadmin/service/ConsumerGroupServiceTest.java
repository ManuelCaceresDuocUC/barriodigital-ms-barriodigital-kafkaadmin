package cl.barriodigital.kafkaadmin.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AlterConsumerGroupOffsetsResult;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.DescribeConsumerGroupsResult;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsResult;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.ListOffsetsResult.ListOffsetsResultInfo;
import org.apache.kafka.clients.admin.MemberAssignment;
import org.apache.kafka.clients.admin.MemberDescription;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.GroupState;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.Uuid;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import cl.barriodigital.kafkaadmin.dto.GroupDetail;
import cl.barriodigital.kafkaadmin.dto.ResetOffsetsRequest;
import cl.barriodigital.kafkaadmin.dto.ResetOffsetsResponse;
import cl.barriodigital.kafkaadmin.dto.ResetStrategy;
import cl.barriodigital.kafkaadmin.exception.GroupActiveException;
import cl.barriodigital.kafkaadmin.exception.GroupNotFoundException;
import cl.barriodigital.kafkaadmin.exception.InvalidKafkaRequestException;

class ConsumerGroupServiceTest {

    private static final TopicPartition P0 = new TopicPartition("requests.events", 0);
    private static final TopicPartition P1 = new TopicPartition("requests.events", 1);

    private Admin admin;
    private ConsumerGroupService service;

    @BeforeEach
    void setUp() {
        admin = mock(Admin.class);
        service = new ConsumerGroupService(admin, KafkaTestSupport.calls());
    }

    private void groupIs(String groupId, GroupState state, List<MemberDescription> members) {
        ConsumerGroupDescription description = mock(ConsumerGroupDescription.class);
        when(description.groupState()).thenReturn(state);
        when(description.members()).thenReturn(members);
        when(description.coordinator()).thenReturn(new Node(1, "kafka-1", 29092));
        DescribeConsumerGroupsResult result = mock(DescribeConsumerGroupsResult.class);
        when(result.all()).thenReturn(KafkaFuture.completedFuture(Map.of(groupId, description)));
        when(admin.describeConsumerGroups(anyCollection())).thenReturn(result);
    }

    private static ListOffsetsResultInfo info(long offset) {
        return new ListOffsetsResultInfo(offset, 0L, java.util.Optional.empty());
    }

    private void endOffsetsAre(Map<TopicPartition, ListOffsetsResultInfo> ends) {
        ListOffsetsResult result = mock(ListOffsetsResult.class);
        when(result.all()).thenReturn(KafkaFuture.completedFuture(ends));
        when(admin.listOffsets(anyMap())).thenReturn(result);
    }

    @Test
    void detalleCalculaElLagPorParticionYTotal() {
        MemberDescription member = mock(MemberDescription.class);
        when(member.consumerId()).thenReturn("consumer-1");
        when(member.clientId()).thenReturn("report");
        when(member.host()).thenReturn("/10.0.0.5");
        when(member.assignment()).thenReturn(new MemberAssignment(Set.of(P0, P1)));
        groupIs("report-group", GroupState.STABLE, List.of(member));

        ListConsumerGroupOffsetsResult offsets = mock(ListConsumerGroupOffsetsResult.class);
        when(offsets.partitionsToOffsetAndMetadata()).thenReturn(KafkaFuture.completedFuture(
                Map.of(P0, new OffsetAndMetadata(10), P1, new OffsetAndMetadata(4))));
        when(admin.listConsumerGroupOffsets(eq("report-group"))).thenReturn(offsets);
        endOffsetsAre(Map.of(P0, info(15), P1, info(4)));

        GroupDetail detail = service.get("report-group");

        assertThat(detail.state()).isEqualTo("STABLE");
        assertThat(detail.members().get(0).assignedPartitions())
                .containsExactly("requests.events-0", "requests.events-1");
        assertThat(detail.offsets()).extracting("lag").containsExactly(5L, 0L);
        assertThat(detail.totalLag()).isEqualTo(5L);
        assertThat(detail.coordinator()).isEqualTo("kafka-1:29092");
    }

    @Test
    void grupoInexistenteSeReportaComoNoEncontrado() {
        groupIs("fantasma", GroupState.DEAD, List.of());

        assertThatThrownBy(() -> service.get("fantasma")).isInstanceOf(GroupNotFoundException.class);
    }

    @Test
    void noReseteaSiElGrupoTieneConsumidoresActivos() {
        groupIs("report-group", GroupState.STABLE, List.of());

        assertThatThrownBy(() -> service.resetOffsets("report-group",
                new ResetOffsetsRequest("requests.events", ResetStrategy.EARLIEST, null, null)))
                .isInstanceOf(GroupActiveException.class);
        verify(admin, never()).alterConsumerGroupOffsets(eq("report-group"), anyMap());
    }

    @Test
    void estrategiaOffsetExigeElOffset() {
        groupIs("report-group", GroupState.EMPTY, List.of());

        assertThatThrownBy(() -> service.resetOffsets("report-group",
                new ResetOffsetsRequest("requests.events", ResetStrategy.OFFSET, null, null)))
                .isInstanceOf(InvalidKafkaRequestException.class);
    }

    @Test
    void reseteaAlInicioTodasLasParticionesDelTopico() {
        groupIs("report-group", GroupState.EMPTY, List.of());
        Node n = new Node(1, "kafka-1", 29092);
        TopicDescription topic = new TopicDescription("requests.events", false, List.of(
                new TopicPartitionInfo(0, n, List.of(n), List.of(n)),
                new TopicPartitionInfo(1, n, List.of(n), List.of(n))), null, Uuid.randomUuid());
        DescribeTopicsResult described = mock(DescribeTopicsResult.class);
        when(described.allTopicNames()).thenReturn(KafkaFuture.completedFuture(Map.of("requests.events", topic)));
        when(admin.describeTopics(anyCollection())).thenReturn(described);
        endOffsetsAre(Map.of(P0, info(0), P1, info(2)));
        AlterConsumerGroupOffsetsResult altered = mock(AlterConsumerGroupOffsetsResult.class);
        when(altered.all()).thenReturn(KafkaFuture.completedFuture(null));
        when(admin.alterConsumerGroupOffsets(eq("report-group"), anyMap())).thenReturn(altered);

        ResetOffsetsResponse response = service.resetOffsets("report-group",
                new ResetOffsetsRequest("requests.events", ResetStrategy.EARLIEST, null, null));

        @SuppressWarnings("unchecked")
        ArgumentCaptor<Map<TopicPartition, OffsetAndMetadata>> captor = ArgumentCaptor.forClass(Map.class);
        verify(admin).alterConsumerGroupOffsets(eq("report-group"), captor.capture());
        assertThat(captor.getValue()).containsOnlyKeys(P0, P1);
        assertThat(response.offsets()).extracting("partition").containsExactly(0, 1);
    }

    @Test
    void particionInexistenteEsPeticionInvalida() {
        groupIs("report-group", GroupState.EMPTY, List.of());
        Node n = new Node(1, "kafka-1", 29092);
        TopicDescription topic = new TopicDescription("requests.events", false,
                List.of(new TopicPartitionInfo(0, n, List.of(n), List.of(n))), null, Uuid.randomUuid());
        DescribeTopicsResult described = mock(DescribeTopicsResult.class);
        when(described.allTopicNames()).thenReturn(KafkaFuture.completedFuture(Map.of("requests.events", topic)));
        when(admin.describeTopics(anyCollection())).thenReturn(described);

        assertThatThrownBy(() -> service.resetOffsets("report-group",
                new ResetOffsetsRequest("requests.events", ResetStrategy.OFFSET, 3L, 9)))
                .isInstanceOf(InvalidKafkaRequestException.class);
    }
}
