package cl.barriodigital.kafkaadmin.dto;

import java.util.List;

/**
 * QUÉ ES: DTO de salida de GET /api/kafka/cluster: identificador del clúster, cuál broker es el
 * "controlador" (el que coordina elecciones de líder) y la lista de brokers.
 */
public record ClusterResponse(String clusterId, Integer controllerId, List<BrokerInfo> brokers) {
}
