package cl.barriodigital.kafkaadmin.exception;

import java.time.Instant;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.method.annotation.MethodArgumentTypeMismatchException;

import cl.barriodigital.kafkaadmin.dto.ApiError;
import lombok.extern.slf4j.Slf4j;

/**
 * Traduce las excepciones de dominio y de entrada a respuestas JSON con un {@code message} legible.
 *
 * <p>QUÉ ES: manejador global de errores (@RestControllerAdvice). Mapa de códigos:
 * 404 = no existe; 409 = conflicto (ya existe, protegido, grupo activo); 400 = petición inválida;
 * 502 = Kafka falló o no respondió.
 */
@Slf4j
@RestControllerAdvice
public class GlobalExceptionHandler {

    // 404: tópico o grupo inexistente.
    @ExceptionHandler({ TopicNotFoundException.class, GroupNotFoundException.class })
    public ResponseEntity<ApiError> handleNotFound(KafkaAdminException ex) {
        return build(HttpStatus.NOT_FOUND, ex.getMessage(), null);
    }

    // 409: el estado actual impide la operación.
    @ExceptionHandler({ TopicAlreadyExistsException.class, ProtectedTopicException.class,
            GroupHasMembersException.class, GroupActiveException.class })
    public ResponseEntity<ApiError> handleConflict(KafkaAdminException ex) {
        return build(HttpStatus.CONFLICT, ex.getMessage(), null);
    }

    // 400: datos rechazados por las reglas del servicio o por Kafka.
    @ExceptionHandler(InvalidKafkaRequestException.class)
    public ResponseEntity<ApiError> handleInvalid(InvalidKafkaRequestException ex) {
        return build(HttpStatus.BAD_REQUEST, ex.getMessage(), null);
    }

    /** Kafka no respondió o falló: es un fallo aguas arriba, no del cliente. */
    @ExceptionHandler(KafkaOperationException.class)
    public ResponseEntity<ApiError> handleUpstream(KafkaOperationException ex) {
        // Este sí se registra con stacktrace: es un problema de infraestructura que conviene investigar.
        log.error("Falla al hablar con Kafka: {}", ex.getMessage(), ex);
        return build(HttpStatus.BAD_GATEWAY, ex.getMessage(), null);
    }

    // 400: falló @Valid en el cuerpo; se detalla campo por campo.
    @ExceptionHandler(MethodArgumentNotValidException.class)
    public ResponseEntity<ApiError> handleValidation(MethodArgumentNotValidException ex) {
        // TreeMap: el orden de los campos es estable y el mensaje no cambia entre ejecuciones.
        Map<String, String> fieldErrors = new TreeMap<>();
        ex.getBindingResult().getFieldErrors()
                .forEach(error -> fieldErrors.putIfAbsent(error.getField(), error.getDefaultMessage()));
        String message = "Datos inválidos: " + fieldErrors.entrySet().stream()
                .map(entry -> entry.getKey() + ": " + entry.getValue())
                .collect(Collectors.joining("; "));
        return build(HttpStatus.BAD_REQUEST, message, fieldErrors);
    }

    // 400: el cuerpo no es JSON válido (o un enum con valor desconocido).
    @ExceptionHandler(HttpMessageNotReadableException.class)
    public ResponseEntity<ApiError> handleUnreadable(HttpMessageNotReadableException ex) {
        return build(HttpStatus.BAD_REQUEST, "El cuerpo de la solicitud no es un JSON válido", null);
    }

    // 400: un parámetro de la URL tiene un tipo incorrecto.
    @ExceptionHandler(MethodArgumentTypeMismatchException.class)
    public ResponseEntity<ApiError> handleTypeMismatch(MethodArgumentTypeMismatchException ex) {
        return build(HttpStatus.BAD_REQUEST, "El parámetro '" + ex.getName() + "' no tiene un valor válido", null);
    }

    // Construye el ApiError uniforme.
    private ResponseEntity<ApiError> build(HttpStatus status, String message, Map<String, String> fieldErrors) {
        ApiError body = new ApiError(Instant.now(), status.value(), status.getReasonPhrase(), message, fieldErrors);
        return ResponseEntity.status(status).body(body);
    }
}
