package cl.barriodigital.kafkaadmin.util;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.errors.GroupIdNotFoundException;
import org.apache.kafka.common.errors.GroupNotEmptyException;
import org.apache.kafka.common.errors.InvalidConfigurationException;
import org.apache.kafka.common.errors.InvalidPartitionsException;
import org.apache.kafka.common.errors.InvalidReplicationFactorException;
import org.apache.kafka.common.errors.InvalidRequestException;
import org.apache.kafka.common.errors.InvalidTopicException;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.springframework.stereotype.Component;

import cl.barriodigital.kafkaadmin.config.KafkaConnectionProperties;
import cl.barriodigital.kafkaadmin.exception.GroupHasMembersException;
import cl.barriodigital.kafkaadmin.exception.GroupNotFoundException;
import cl.barriodigital.kafkaadmin.exception.InvalidKafkaRequestException;
import cl.barriodigital.kafkaadmin.exception.KafkaOperationException;
import cl.barriodigital.kafkaadmin.exception.TopicAlreadyExistsException;
import cl.barriodigital.kafkaadmin.exception.TopicNotFoundException;

/** Espera el resultado de una operación de Kafka y traduce sus errores a excepciones de dominio. */
@Component
public class KafkaCalls {

    private final long timeoutMillis;

    public KafkaCalls(KafkaConnectionProperties properties) {
        // Pequeño margen sobre el timeout del cliente, que es el que normalmente vence primero.
        this.timeoutMillis = properties.timeout().toMillis() + 2_000;
    }

    /**
     * @param action   lo que se intentaba hacer, para el mensaje de error (por ejemplo "crear el tópico")
     * @param resource tópico o grupo involucrado, para los mensajes de "no existe" / "ya existe"
     */
    public <T> T await(KafkaFuture<T> future, String action, String resource) {
        try {
            return future.get(timeoutMillis, TimeUnit.MILLISECONDS);
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new KafkaOperationException("No se pudo " + action + ": la operación fue interrumpida", ex);
        } catch (java.util.concurrent.TimeoutException ex) {
            throw new KafkaOperationException("No se pudo " + action + ": Kafka no respondió a tiempo", ex);
        } catch (ExecutionException ex) {
            throw translate(ex.getCause() == null ? ex : ex.getCause(), action, resource);
        }
    }

    static RuntimeException translate(Throwable cause, String action, String resource) {
        if (cause instanceof UnknownTopicOrPartitionException) {
            return new TopicNotFoundException(resource);
        }
        if (cause instanceof TopicExistsException) {
            return new TopicAlreadyExistsException(resource);
        }
        if (cause instanceof GroupIdNotFoundException) {
            return new GroupNotFoundException(resource);
        }
        if (cause instanceof GroupNotEmptyException) {
            return new GroupHasMembersException(resource);
        }
        if (cause instanceof InvalidPartitionsException || cause instanceof InvalidReplicationFactorException
                || cause instanceof InvalidConfigurationException || cause instanceof InvalidTopicException
                || cause instanceof InvalidRequestException) {
            return new InvalidKafkaRequestException(cause.getMessage());
        }
        if (cause instanceof org.apache.kafka.common.errors.TimeoutException) {
            return new KafkaOperationException("No se pudo " + action + ": Kafka no respondió a tiempo", cause);
        }
        return new KafkaOperationException("No se pudo " + action + ": " + cause.getMessage(), cause);
    }
}
