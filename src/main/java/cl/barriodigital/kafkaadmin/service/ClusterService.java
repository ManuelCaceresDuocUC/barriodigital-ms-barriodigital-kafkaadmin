package cl.barriodigital.kafkaadmin.service;

import java.util.Comparator;
import java.util.List;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.common.Node;
import org.springframework.stereotype.Service;

import cl.barriodigital.kafkaadmin.dto.BrokerInfo;
import cl.barriodigital.kafkaadmin.dto.ClusterResponse;
import cl.barriodigital.kafkaadmin.util.KafkaCalls;
import lombok.RequiredArgsConstructor;

/**
 * QUÉ ES: servicio que consulta el estado del clúster Kafka (id, controlador y brokers).
 * QUÉ HACE: usa el cliente Admin de Kafka; es solo lectura. Lo llama ClusterController.
 */
@Service
@RequiredArgsConstructor
public class ClusterService {

    // Cliente administrativo de Kafka (lo crea KafkaAdminConfig).
    private final Admin admin;
    // Espera las respuestas asíncronas y traduce errores de Kafka.
    private final KafkaCalls kafka;

    public ClusterResponse describe() {
        // Pide la descripción al clúster; cada dato llega como un KafkaFuture que se espera con kafka.await.
        DescribeClusterResult result = admin.describeCluster();
        String clusterId = kafka.await(result.clusterId(), "consultar el clúster", "");
        Node controller = kafka.await(result.controller(), "consultar el controlador", "");
        // Brokers ordenados por id y convertidos a DTO.
        List<BrokerInfo> brokers = kafka.await(result.nodes(), "consultar los brokers", "").stream()
                .sorted(Comparator.comparingInt(Node::id))
                .map(node -> new BrokerInfo(node.id(), node.host(), node.port(), node.rack()))
                .toList();
        // Puede no haber controlador en un instante de elección: se devuelve null.
        Integer controllerId = controller == null || controller.isEmpty() ? null : controller.id();
        return new ClusterResponse(clusterId, controllerId, brokers);
    }
}
