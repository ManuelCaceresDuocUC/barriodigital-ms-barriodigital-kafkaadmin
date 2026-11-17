package cl.barriodigital.kafkaadmin.dto;

/** QUÉ ES: DTO de salida de la lista de grupos: solo el nombre y el estado de cada uno. */
public record GroupSummary(String groupId, String state) {
}
