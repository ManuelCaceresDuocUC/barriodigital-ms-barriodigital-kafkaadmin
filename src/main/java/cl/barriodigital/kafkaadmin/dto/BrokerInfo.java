package cl.barriodigital.kafkaadmin.dto;

/**
 * QUÉ ES: DTO de salida con los datos de un broker (servidor Kafka) del clúster.
 * @param id   número del broker (1, 2 o 3 en este proyecto)
 * @param rack zona/rack del broker, si está configurada (puede ser null)
 */
public record BrokerInfo(int id, String host, int port, String rack) {
}
