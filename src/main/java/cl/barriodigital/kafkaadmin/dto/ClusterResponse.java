package cl.barriodigital.kafkaadmin.dto;

import java.util.List;

public record ClusterResponse(String clusterId, Integer controllerId, List<BrokerInfo> brokers) {
}
