package cl.barriodigital.kafkaadmin.controller;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import cl.barriodigital.kafkaadmin.dto.ClusterResponse;
import cl.barriodigital.kafkaadmin.service.ClusterService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.RequiredArgsConstructor;

@RestController
@RequestMapping("/api/kafka/cluster")
@RequiredArgsConstructor
@Tag(name = "Clúster", description = "Estado del clúster de Kafka")
public class ClusterController {

    private final ClusterService cluster;

    @GetMapping
    @Operation(summary = "Brokers y controlador del clúster")
    public ClusterResponse describe() {
        return cluster.describe();
    }
}
