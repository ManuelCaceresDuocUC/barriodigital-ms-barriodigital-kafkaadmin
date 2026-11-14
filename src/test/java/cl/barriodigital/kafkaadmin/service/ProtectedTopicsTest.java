package cl.barriodigital.kafkaadmin.service;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class ProtectedTopicsTest {

    @Test
    void reconoceNombresExactosYComodines() {
        ProtectedTopics protectedTopics = KafkaTestSupport.protectedTopics("requests.events", "internal.*");

        assertThat(protectedTopics.isProtected("requests.events")).isTrue();
        assertThat(protectedTopics.isProtected("internal.algo")).isTrue();
        assertThat(protectedTopics.isProtected("otro.topico")).isFalse();
        assertThat(protectedTopics.isProtected(null)).isFalse();
    }

    @Test
    void sinPatronesNadaEstaProtegido() {
        assertThat(KafkaTestSupport.protectedTopics().isProtected("requests.events")).isFalse();
    }
}
