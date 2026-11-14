package cl.barriodigital.kafkaadmin.dto;

public record BrokerInfo(int id, String host, int port, String rack) {
}
