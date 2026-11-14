package cl.barriodigital.kafkaadmin.dto;

/** Hacia dónde mover los offsets de un grupo: inicio del log, fin del log o un offset concreto. */
public enum ResetStrategy {
    EARLIEST,
    LATEST,
    OFFSET
}
