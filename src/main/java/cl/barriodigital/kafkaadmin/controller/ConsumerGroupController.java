package cl.barriodigital.kafkaadmin.controller;

import java.util.List;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import cl.barriodigital.kafkaadmin.dto.GroupDetail;
import cl.barriodigital.kafkaadmin.dto.GroupSummary;
import cl.barriodigital.kafkaadmin.dto.ResetOffsetsRequest;
import cl.barriodigital.kafkaadmin.dto.ResetOffsetsResponse;
import cl.barriodigital.kafkaadmin.service.ConsumerGroupService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;

/**
 * QUÉ ES: controlador REST de los consumer groups (/api/kafka/groups). Solo traduce HTTP <-> servicio.
 * RECORDATORIO: un consumer group es un conjunto de consumidores que se reparten las particiones de un tópico;
 * Kafka guarda por grupo hasta dónde leyó (el offset). En esta app: ms-report-consumer-group-v2 y ms-audit-consumer-group.
 */
@RestController
@RequestMapping("/api/kafka/groups")
@RequiredArgsConstructor
@Tag(name = "Consumer groups", description = "Consulta y gestión de grupos de consumidores")
public class ConsumerGroupController {

    private final ConsumerGroupService groups;

    // GET /api/kafka/groups -> lista de grupos con su estado.
    @GetMapping
    @Operation(summary = "Listar los consumer groups")
    public List<GroupSummary> list() {
        return groups.list();
    }

    // GET /api/kafka/groups/{groupId} -> miembros, offsets por partición y lag (mensajes pendientes).
    @GetMapping("/{groupId}")
    @Operation(summary = "Detalle de un grupo: miembros, offsets y lag")
    public GroupDetail get(@PathVariable String groupId) {
        return groups.get(groupId);
    }

    // POST /api/kafka/groups/{groupId}/offsets/reset -> mover la posición de lectura (volver a leer, saltar...).
    @PostMapping("/{groupId}/offsets/reset")
    @Operation(summary = "Resetear los offsets de un grupo (debe estar EMPTY)")
    public ResetOffsetsResponse resetOffsets(@PathVariable String groupId,
            @Valid @RequestBody ResetOffsetsRequest request) {
        return groups.resetOffsets(groupId, request);
    }

    // DELETE /api/kafka/groups/{groupId} -> 204; falla con 409 si el grupo aún tiene consumidores activos.
    @DeleteMapping("/{groupId}")
    @Operation(summary = "Eliminar un grupo sin consumidores activos")
    public ResponseEntity<Void> delete(@PathVariable String groupId) {
        groups.delete(groupId);
        return ResponseEntity.noContent().build();
    }
}
