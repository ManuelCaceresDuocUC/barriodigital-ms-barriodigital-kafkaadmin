package cl.barriodigital.kafkaadmin.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.CreatePartitionsResult;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.DeleteTopicsResult;
import org.apache.kafka.clients.admin.DescribeConfigsResult;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.config.ConfigResource;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import cl.barriodigital.kafkaadmin.dto.CreateTopicRequest;
import cl.barriodigital.kafkaadmin.dto.TopicDetail;
import cl.barriodigital.kafkaadmin.dto.TopicSummary;
import cl.barriodigital.kafkaadmin.exception.InvalidKafkaRequestException;
import cl.barriodigital.kafkaadmin.exception.ProtectedTopicException;

class TopicServiceTest {

    private static final Node N1 = new Node(1, "kafka-1", 29092);
    private static final Node N2 = new Node(2, "kafka-2", 29092);
    private static final Node N3 = new Node(3, "kafka-3", 29092);

    private Admin admin;
    private TopicService service;

    @BeforeEach
    void setUp() {
        admin = mock(Admin.class);
        service = new TopicService(admin, KafkaTestSupport.calls(), KafkaTestSupport.protectedTopics("requests.events"));
    }

    private static TopicDescription description(String name, int partitions) {
        List<TopicPartitionInfo> infos = new java.util.ArrayList<>();
        for (int i = 0; i < partitions; i++) {
            infos.add(new TopicPartitionInfo(i, N1, List.of(N1, N2, N3), List.of(N1, N2, N3)));
        }
        return new TopicDescription(name, false, infos, null, Uuid.randomUuid());
    }

    private void describes(String name, int partitions) {
        DescribeTopicsResult result = mock(DescribeTopicsResult.class);
        when(result.allTopicNames()).thenReturn(KafkaFuture.completedFuture(Map.of(name, description(name, partitions))));
        when(admin.describeTopics(anyCollection())).thenReturn(result);
        DescribeConfigsResult configs = mock(DescribeConfigsResult.class);
        ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, name);
        Config config = new Config(List.of(new ConfigEntry("retention.ms", "604800000")));
        when(configs.all()).thenReturn(KafkaFuture.completedFuture(Map.of(resource, config)));
        when(admin.describeConfigs(anyCollection())).thenReturn(configs);
    }

    @Test
    void creaElTopicoConSusParticionesYConfiguracion() {
        CreateTopicsResult result = mock(CreateTopicsResult.class);
        when(result.all()).thenReturn(KafkaFuture.completedFuture(null));
        when(admin.createTopics(anyCollection())).thenReturn(result);

        TopicSummary summary = service.create(
                new CreateTopicRequest("pedidos.eventos", 3, 3, Map.of("retention.ms", "1000")));

        @SuppressWarnings("unchecked")
        ArgumentCaptor<java.util.Collection<NewTopic>> captor = ArgumentCaptor.forClass(java.util.Collection.class);
        verify(admin).createTopics(captor.capture());
        NewTopic created = captor.getValue().iterator().next();
        assertThat(created.name()).isEqualTo("pedidos.eventos");
        assertThat(created.numPartitions()).isEqualTo(3);
        assertThat(created.replicationFactor()).isEqualTo((short) 3);
        assertThat(created.configs()).containsEntry("retention.ms", "1000");
        assertThat(summary.protectedTopic()).isFalse();
    }

    @Test
    void noCreaSiLaConfiguracionNoEstaPermitida() {
        assertThatThrownBy(() -> service.create(new CreateTopicRequest("t", 1, 1, Map.of("unclean.leader.election.enable", "true"))))
                .isInstanceOf(InvalidKafkaRequestException.class);
        verify(admin, never()).createTopics(anyCollection());
    }

    @Test
    void listaLosTopicosOrdenadosYMarcaLosProtegidos() {
        ListTopicsResult listed = mock(ListTopicsResult.class);
        when(listed.names()).thenReturn(KafkaFuture.completedFuture(Set.of("b.topic", "requests.events")));
        when(admin.listTopics()).thenReturn(listed);
        DescribeTopicsResult described = mock(DescribeTopicsResult.class);
        when(described.allTopicNames()).thenReturn(KafkaFuture.completedFuture(
                Map.of("b.topic", description("b.topic", 1), "requests.events", description("requests.events", 3))));
        when(admin.describeTopics(anyCollection())).thenReturn(described);

        List<TopicSummary> topics = service.list();

        assertThat(topics).extracting(TopicSummary::name).containsExactly("b.topic", "requests.events");
        assertThat(topics.get(1).partitions()).isEqualTo(3);
        assertThat(topics.get(1).replicationFactor()).isEqualTo(3);
        assertThat(topics.get(1).protectedTopic()).isTrue();
    }

    @Test
    void listaVaciaSinConsultarDetalle() {
        ListTopicsResult listed = mock(ListTopicsResult.class);
        when(listed.names()).thenReturn(KafkaFuture.completedFuture(Set.of()));
        when(admin.listTopics()).thenReturn(listed);

        assertThat(service.list()).isEmpty();
        verify(admin, never()).describeTopics(anyCollection());
    }

    @Test
    void detalleIncluyeLiderIsrYConfiguracion() {
        describes("requests.events", 3);

        TopicDetail detail = service.get("requests.events");

        assertThat(detail.partitionCount()).isEqualTo(3);
        assertThat(detail.protectedTopic()).isTrue();
        assertThat(detail.partitions().get(0).leader()).isEqualTo(1);
        assertThat(detail.partitions().get(0).isr()).containsExactly(1, 2, 3);
        assertThat(detail.configs()).extracting("name").contains("retention.ms");
    }

    @Test
    void ampliaParticionesSoloHaciaArriba() {
        describes("t", 3);
        CreatePartitionsResult result = mock(CreatePartitionsResult.class);
        when(result.all()).thenReturn(KafkaFuture.completedFuture(null));
        when(admin.createPartitions(anyMap())).thenReturn(result);

        service.increasePartitions("t", 6);
        verify(admin).createPartitions(anyMap());

        assertThatThrownBy(() -> service.increasePartitions("t", 3)).isInstanceOf(InvalidKafkaRequestException.class);
        assertThatThrownBy(() -> service.increasePartitions("t", 2)).isInstanceOf(InvalidKafkaRequestException.class);
    }

    @Test
    void noEliminaTopicosDelSistema() {
        assertThatThrownBy(() -> service.delete("requests.events")).isInstanceOf(ProtectedTopicException.class);
        verify(admin, never()).deleteTopics(anyCollection());
    }

    @Test
    void eliminaUnTopicoNormal() {
        DeleteTopicsResult result = mock(DeleteTopicsResult.class);
        when(result.all()).thenReturn(KafkaFuture.completedFuture(null));
        when(admin.deleteTopics(anyCollection())).thenReturn(result);

        service.delete("temporal");

        verify(admin).deleteTopics(any(java.util.Collection.class));
    }
}
