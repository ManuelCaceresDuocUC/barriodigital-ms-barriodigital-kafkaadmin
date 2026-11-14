package cl.barriodigital.kafkaadmin.util;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;

import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.errors.GroupIdNotFoundException;
import org.apache.kafka.common.errors.GroupNotEmptyException;
import org.apache.kafka.common.errors.InvalidPartitionsException;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.internals.KafkaFutureImpl;
import org.junit.jupiter.api.Test;

import cl.barriodigital.kafkaadmin.config.KafkaConnectionProperties;
import cl.barriodigital.kafkaadmin.exception.GroupHasMembersException;
import cl.barriodigital.kafkaadmin.exception.GroupNotFoundException;
import cl.barriodigital.kafkaadmin.exception.InvalidKafkaRequestException;
import cl.barriodigital.kafkaadmin.exception.KafkaOperationException;
import cl.barriodigital.kafkaadmin.exception.TopicAlreadyExistsException;
import cl.barriodigital.kafkaadmin.exception.TopicNotFoundException;

class KafkaCallsTest {

    private final KafkaCalls calls = new KafkaCalls(new KafkaConnectionProperties("x:9092", Duration.ofMillis(100)));

    private static KafkaFuture<String> failed(Throwable error) {
        KafkaFutureImpl<String> future = new KafkaFutureImpl<>();
        future.completeExceptionally(error);
        return future;
    }

    @Test
    void devuelveElResultadoCuandoKafkaResponde() {
        assertThat(calls.await(KafkaFuture.completedFuture("ok"), "hacer algo", "t")).isEqualTo("ok");
    }

    @Test
    void traduceTopicoInexistente() {
        assertThatThrownBy(() -> calls.await(failed(new UnknownTopicOrPartitionException()), "x", "t1"))
                .isInstanceOf(TopicNotFoundException.class).hasMessageContaining("t1");
    }

    @Test
    void traduceTopicoDuplicado() {
        assertThatThrownBy(() -> calls.await(failed(new TopicExistsException("dup")), "x", "t1"))
                .isInstanceOf(TopicAlreadyExistsException.class);
    }

    @Test
    void traduceGrupoInexistenteYGrupoConMiembros() {
        assertThatThrownBy(() -> calls.await(failed(new GroupIdNotFoundException("g")), "x", "g"))
                .isInstanceOf(GroupNotFoundException.class);
        assertThatThrownBy(() -> calls.await(failed(new GroupNotEmptyException("g")), "x", "g"))
                .isInstanceOf(GroupHasMembersException.class);
    }

    @Test
    void erroresDeValidacionDeKafkaSonPeticionInvalida() {
        assertThatThrownBy(() -> calls.await(failed(new InvalidPartitionsException("menos")), "x", "t"))
                .isInstanceOf(InvalidKafkaRequestException.class).hasMessageContaining("menos");
    }

    @Test
    void otrosErroresSonFallaAguasArriba() {
        assertThatThrownBy(() -> calls.await(failed(new IllegalStateException("boom")), "crear el tópico", "t"))
                .isInstanceOf(KafkaOperationException.class)
                .hasMessageContaining("crear el tópico").hasMessageContaining("boom");
    }

    @Test
    void siKafkaNoRespondeAEsperaVenceElTimeout() {
        KafkaFutureImpl<String> never = new KafkaFutureImpl<>();
        assertThatThrownBy(() -> calls.await(never, "listar", ""))
                .isInstanceOf(KafkaOperationException.class).hasMessageContaining("no respondió a tiempo");
    }
}
