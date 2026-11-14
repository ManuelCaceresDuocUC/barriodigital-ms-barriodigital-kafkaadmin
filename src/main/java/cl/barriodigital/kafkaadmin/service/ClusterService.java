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

/** Estado del clúster: id, controlador y brokers. */
@Service
@RequiredArgsConstructor
public class ClusterService {

    private final Admin admin;
    private final KafkaCalls kafka;

    public ClusterResponse describe() {
        DescribeClusterResult result = admin.describeCluster();
        String clusterId = kafka.await(result.clusterId(), "consultar el clúster", "");
        Node controller = kafka.await(result.controller(), "consultar el controlador", "");
        List<BrokerInfo> brokers = kafka.await(result.nodes(), "consultar los brokers", "").stream()
                .sorted(Comparator.comparingInt(Node::id))
                .map(node -> new BrokerInfo(node.id(), node.host(), node.port(), node.rack()))
                .toList();
        Integer controllerId = controller == null || controller.isEmpty() ? null : controller.id();
        return new ClusterResponse(clusterId, controllerId, brokers);
    }
}
