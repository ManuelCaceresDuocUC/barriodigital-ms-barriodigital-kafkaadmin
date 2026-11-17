package cl.barriodigital.kafkaadmin.controller;

import java.util.List;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import cl.barriodigital.kafkaadmin.dto.CreateTopicRequest;
import cl.barriodigital.kafkaadmin.dto.IncreasePartitionsRequest;
import cl.barriodigital.kafkaadmin.dto.ProvisionResult;
import cl.barriodigital.kafkaadmin.dto.TopicDetail;
import cl.barriodigital.kafkaadmin.dto.TopicSummary;
import cl.barriodigital.kafkaadmin.dto.UpdateTopicConfigRequest;
import cl.barriodigital.kafkaadmin.service.TopicProvisioner;
import cl.barriodigital.kafkaadmin.service.TopicService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;

/**
 * QUÉ ES: controlador REST de tópicos (/api/kafka/topics): crear, listar, ver detalle, cambiar configuración,
 * aumentar particiones, eliminar y aprovisionar los tópicos del sistema. La lógica está en TopicService.
 */
@RestController
@RequestMapping("/api/kafka/topics")
@RequiredArgsConstructor
@Tag(name = "Tópicos", description = "Administración de tópicos de Kafka")
public class TopicController {

    private final TopicService topics;
    private final TopicProvisioner provisioner;

    // POST /api/kafka/topics -> 201 Created. @Valid aplica las validaciones del DTO (nombre, particiones...).
    @PostMapping
    @Operation(summary = "Crear un tópico")
    public ResponseEntity<TopicSummary> create(@Valid @RequestBody CreateTopicRequest request) {
        return ResponseEntity.status(HttpStatus.CREATED).body(topics.create(request));
    }

    // GET /api/kafka/topics -> lista de tópicos (nombre, particiones, réplicas).
    @GetMapping
    @Operation(summary = "Listar los tópicos")
    public List<TopicSummary> list() {
        return topics.list();
    }

    /** Declarado antes que /{name}: "provision" no es un nombre de tópico. */
    // POST /api/kafka/topics/provision -> crea los tópicos del sistema que falten (los mismos que al arrancar).
    @PostMapping("/provision")
    @Operation(summary = "Crear los tópicos del sistema que falten (idempotente)")
    public List<ProvisionResult> provision() {
        return provisioner.provision();
    }

    // GET /api/kafka/topics/{name} -> por partición: líder, réplicas y réplicas sincronizadas (ISR); más la configuración.
    @GetMapping("/{name}")
    @Operation(summary = "Detalle de un tópico: particiones, líder, ISR y configuración")
    public TopicDetail get(@PathVariable String name) {
        return topics.get(name);
    }

    // PUT /api/kafka/topics/{name}/config -> cambia configuraciones permitidas (retención, min.insync.replicas...).
    @PutMapping("/{name}/config")
    @Operation(summary = "Modificar la configuración de un tópico")
    public TopicDetail updateConfig(@PathVariable String name, @Valid @RequestBody UpdateTopicConfigRequest request) {
        return topics.updateConfig(name, request.configs());
    }

    // PUT /api/kafka/topics/{name}/partitions -> solo se pueden AUMENTAR (Kafka no permite reducirlas).
    @PutMapping("/{name}/partitions")
    @Operation(summary = "Aumentar las particiones de un tópico")
    public TopicDetail increasePartitions(@PathVariable String name,
            @Valid @RequestBody IncreasePartitionsRequest request) {
        return topics.increasePartitions(name, request.partitions());
    }

    // DELETE /api/kafka/topics/{name} -> 204; los tópicos protegidos responden 409.
    @DeleteMapping("/{name}")
    @Operation(summary = "Eliminar un tópico (los del sistema están protegidos)")
    public ResponseEntity<Void> delete(@PathVariable String name) {
        topics.delete(name);
        return ResponseEntity.noContent().build();
    }
}
