package cl.barriodigital.kafkaadmin.dto;

/**
 * Hacia dónde mover los offsets de un grupo: inicio del log, fin del log o un offset concreto.
 * EARLIEST = releer todo; LATEST = saltar lo pendiente y leer solo lo nuevo; OFFSET = una posición exacta.
 */
public enum ResetStrategy {
    EARLIEST,
    LATEST,
    OFFSET
}
