package cl.barriodigital.kafkaadmin.service;

import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.NewPartitions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.config.ConfigResource;
import org.springframework.stereotype.Service;

import cl.barriodigital.kafkaadmin.dto.ConfigDetail;
import cl.barriodigital.kafkaadmin.dto.CreateTopicRequest;
import cl.barriodigital.kafkaadmin.dto.PartitionDetail;
import cl.barriodigital.kafkaadmin.dto.TopicDetail;
import cl.barriodigital.kafkaadmin.dto.TopicSummary;
import cl.barriodigital.kafkaadmin.exception.InvalidKafkaRequestException;
import cl.barriodigital.kafkaadmin.exception.ProtectedTopicException;
import cl.barriodigital.kafkaadmin.util.KafkaCalls;
import cl.barriodigital.kafkaadmin.util.TopicConfigRules;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/** Administración de tópicos: crear, consultar, configurar, ampliar particiones y eliminar. */
@Slf4j
@Service
@RequiredArgsConstructor
public class TopicService {

    private final Admin admin;
    private final KafkaCalls kafka;
    private final ProtectedTopics protectedTopics;

    public TopicSummary create(CreateTopicRequest request) {
        TopicConfigRules.validate(request.configs());
        NewTopic topic = new NewTopic(request.name(), request.partitions(), request.replicationFactor().shortValue());
        if (request.configs() != null && !request.configs().isEmpty()) {
            topic.configs(Map.copyOf(request.configs()));
        }
        kafka.await(admin.createTopics(List.of(topic)).all(), "crear el tópico", request.name());
        log.info("Tópico '{}' creado ({} particiones, {} réplicas)", request.name(), request.partitions(),
                request.replicationFactor());
        return new TopicSummary(request.name(), request.partitions(), request.replicationFactor(),
                protectedTopics.isProtected(request.name()));
    }

    public List<TopicSummary> list() {
        Set<String> names = kafka.await(admin.listTopics().names(), "listar los tópicos", "");
        if (names.isEmpty()) {
            return List.of();
        }
        Map<String, TopicDescription> descriptions =
                kafka.await(admin.describeTopics(List.copyOf(names)).allTopicNames(), "describir los tópicos", "");
        return descriptions.values().stream()
                .sorted(Comparator.comparing(TopicDescription::name))
                .map(description -> new TopicSummary(description.name(), description.partitions().size(),
                        replicationFactorOf(description), protectedTopics.isProtected(description.name())))
                .toList();
    }

    public TopicDetail get(String name) {
        TopicDescription description = describe(name);
        ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, name);
        Config config = kafka.await(admin.describeConfigs(List.of(resource)).all(), "leer la configuración", name)
                .get(resource);

        List<PartitionDetail> partitions = description.partitions().stream()
                .sorted(Comparator.comparingInt(TopicPartitionInfo::partition))
                .map(info -> new PartitionDetail(info.partition(), idOf(info.leader()), ids(info.replicas()),
                        ids(info.isr())))
                .toList();
        List<ConfigDetail> configs = config == null ? List.of() : config.entries().stream()
                .sorted(Comparator.comparing(ConfigEntry::name))
                .map(entry -> new ConfigDetail(entry.name(), entry.value(), entry.source().name(), entry.isReadOnly()))
                .toList();

        return new TopicDetail(name, partitions.size(), replicationFactorOf(description),
                protectedTopics.isProtected(name), partitions, configs);
    }

    /** Fija las configuraciones indicadas (por ejemplo retention.ms); las demás quedan como estaban. */
    public TopicDetail updateConfig(String name, Map<String, String> configs) {
        TopicConfigRules.validate(configs);
        ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, name);
        // TreeMap: el orden de las operaciones no depende del hash del mapa recibido.
        Collection<AlterConfigOp> operations = new TreeMap<>(configs).entrySet().stream()
                .map(entry -> new AlterConfigOp(new ConfigEntry(entry.getKey(), entry.getValue()),
                        AlterConfigOp.OpType.SET))
                .toList();
        kafka.await(admin.incrementalAlterConfigs(Map.of(resource, operations)).all(),
                "actualizar la configuración", name);
        log.info("Configuración del tópico '{}' actualizada: {}", name, configs.keySet());
        return get(name);
    }

    /** Kafka solo permite aumentar las particiones; reducirlas exige recrear el tópico. */
    public TopicDetail increasePartitions(String name, int total) {
        int current = describe(name).partitions().size();
        if (total <= current) {
            throw new InvalidKafkaRequestException("El tópico '" + name + "' ya tiene " + current
                    + " particiones: el nuevo total debe ser mayor");
        }
        kafka.await(admin.createPartitions(Map.of(name, NewPartitions.increaseTo(total))).all(),
                "ampliar las particiones", name);
        log.info("Tópico '{}' ampliado de {} a {} particiones", name, current, total);
        return get(name);
    }

    public void delete(String name) {
        if (protectedTopics.isProtected(name)) {
            throw new ProtectedTopicException(name);
        }
        kafka.await(admin.deleteTopics(List.of(name)).all(), "eliminar el tópico", name);
        log.info("Tópico '{}' eliminado", name);
    }

    private TopicDescription describe(String name) {
        Map<String, TopicDescription> result =
                kafka.await(admin.describeTopics(List.of(name)).allTopicNames(), "describir el tópico", name);
        return result.get(name);
    }

    private static int replicationFactorOf(TopicDescription description) {
        return description.partitions().isEmpty() ? 0 : description.partitions().get(0).replicas().size();
    }

    private static Integer idOf(Node node) {
        return node == null || node.isEmpty() ? null : node.id();
    }

    private static List<Integer> ids(List<Node> nodes) {
        return nodes.stream().map(Node::id).toList();
    }
}
