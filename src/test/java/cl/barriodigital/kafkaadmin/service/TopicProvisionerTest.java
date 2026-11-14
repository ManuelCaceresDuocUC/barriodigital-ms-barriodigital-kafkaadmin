package cl.barriodigital.kafkaadmin.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.internals.KafkaFutureImpl;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.DefaultApplicationArguments;

import cl.barriodigital.kafkaadmin.config.ProvisioningProperties;
import cl.barriodigital.kafkaadmin.config.ProvisioningProperties.TopicDefinition;
import cl.barriodigital.kafkaadmin.dto.ProvisionResult;

class TopicProvisionerTest {

    private Admin admin;

    @BeforeEach
    void setUp() {
        admin = mock(Admin.class);
    }

    private TopicProvisioner provisioner(boolean enabled) {
        ProvisioningProperties properties = new ProvisioningProperties(enabled,
                List.of(new TopicDefinition("requests.events", 3, 3, Map.of("min.insync.replicas", "2"))));
        return new TopicProvisioner(admin, KafkaTestSupport.calls(), properties);
    }

    @Test
    void creaElTopicoQueFalta() {
        CreateTopicsResult result = mock(CreateTopicsResult.class);
        when(result.all()).thenReturn(KafkaFuture.completedFuture(null));
        when(admin.createTopics(anyCollection())).thenReturn(result);

        assertThat(provisioner(true).provision())
                .containsExactly(new ProvisionResult("requests.events", ProvisionResult.CREATED));
    }

    @Test
    void esIdempotenteSiElTopicoYaExiste() {
        KafkaFutureImpl<Void> failed = new KafkaFutureImpl<>();
        failed.completeExceptionally(new TopicExistsException("ya existe"));
        CreateTopicsResult result = mock(CreateTopicsResult.class);
        when(result.all()).thenReturn(failed);
        when(admin.createTopics(anyCollection())).thenReturn(result);

        assertThat(provisioner(true).provision())
                .containsExactly(new ProvisionResult("requests.events", ProvisionResult.EXISTS));
    }

    @Test
    void alArrancarNoTumbaElServicioSiKafkaFalla() {
        when(admin.createTopics(anyCollection())).thenThrow(new IllegalStateException("sin brokers"));

        provisioner(true).run(new DefaultApplicationArguments());
    }

    @Test
    void deshabilitadoNoHaceNada() {
        provisioner(false).run(new DefaultApplicationArguments());

        verify(admin, never()).createTopics(anyCollection());
    }
}
