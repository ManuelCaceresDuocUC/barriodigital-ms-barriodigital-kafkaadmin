package cl.barriodigital.kafkaadmin.controller;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.delete;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import cl.barriodigital.kafkaadmin.dto.ClusterResponse;
import cl.barriodigital.kafkaadmin.dto.ProvisionResult;
import cl.barriodigital.kafkaadmin.dto.TopicSummary;
import cl.barriodigital.kafkaadmin.exception.GlobalExceptionHandler;
import cl.barriodigital.kafkaadmin.exception.GroupActiveException;
import cl.barriodigital.kafkaadmin.exception.ProtectedTopicException;
import cl.barriodigital.kafkaadmin.exception.TopicNotFoundException;
import cl.barriodigital.kafkaadmin.service.ClusterService;
import cl.barriodigital.kafkaadmin.service.ConsumerGroupService;
import cl.barriodigital.kafkaadmin.service.TopicProvisioner;
import cl.barriodigital.kafkaadmin.service.TopicService;

class KafkaControllersTest {

    private TopicService topics;
    private ConsumerGroupService groups;
    private TopicProvisioner provisioner;
    private ClusterService cluster;
    private MockMvc mvc;

    @BeforeEach
    void setUp() {
        topics = mock(TopicService.class);
        groups = mock(ConsumerGroupService.class);
        provisioner = mock(TopicProvisioner.class);
        cluster = mock(ClusterService.class);
        mvc = MockMvcBuilders.standaloneSetup(new TopicController(topics, provisioner),
                new ConsumerGroupController(groups), new ClusterController(cluster))
                .setControllerAdvice(new GlobalExceptionHandler()).build();
    }

    @Test
    void creaUnTopicoYResponde201() throws Exception {
        when(topics.create(any())).thenReturn(new TopicSummary("pedidos", 3, 3, false));

        mvc.perform(post("/api/kafka/topics").contentType(MediaType.APPLICATION_JSON)
                .content("{\"name\":\"pedidos\",\"partitions\":3,\"replicationFactor\":3}"))
                .andExpect(status().isCreated())
                .andExpect(jsonPath("$.name").value("pedidos"));
    }

    @Test
    void rechazaUnNombreInvalidoConMensajeLegible() throws Exception {
        mvc.perform(post("/api/kafka/topics").contentType(MediaType.APPLICATION_JSON)
                .content("{\"name\":\"con espacio\",\"partitions\":3,\"replicationFactor\":3}"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.message").value(org.hamcrest.Matchers.containsString("name")));
    }

    @Test
    void rechazaMasReplicasQueBrokers() throws Exception {
        mvc.perform(post("/api/kafka/topics").contentType(MediaType.APPLICATION_JSON)
                .content("{\"name\":\"t\",\"partitions\":3,\"replicationFactor\":5}"))
                .andExpect(status().isBadRequest());
    }

    @Test
    void topicoInexistenteEs404() throws Exception {
        when(topics.get("nada")).thenThrow(new TopicNotFoundException("nada"));

        mvc.perform(get("/api/kafka/topics/nada")).andExpect(status().isNotFound());
    }

    @Test
    void eliminarUnTopicoProtegidoEs409() throws Exception {
        doThrow(new ProtectedTopicException("requests.events")).when(topics).delete("requests.events");

        mvc.perform(delete("/api/kafka/topics/requests.events")).andExpect(status().isConflict());
    }

    @Test
    void provisionNoSeConfundeConUnNombreDeTopico() throws Exception {
        when(provisioner.provision()).thenReturn(List.of(new ProvisionResult("requests.events", "EXISTS")));

        mvc.perform(post("/api/kafka/topics/provision"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("0.status").value("EXISTS"));
    }

    @Test
    void resetearUnGrupoActivoEs409() throws Exception {
        when(groups.resetOffsets(eq("g"), any())).thenThrow(new GroupActiveException("g", "STABLE"));

        mvc.perform(post("/api/kafka/groups/g/offsets/reset").contentType(MediaType.APPLICATION_JSON)
                .content("{\"topic\":\"requests.events\",\"strategy\":\"EARLIEST\"}"))
                .andExpect(status().isConflict());
    }

    @Test
    void resetearSinEstrategiaEs400() throws Exception {
        mvc.perform(post("/api/kafka/groups/g/offsets/reset").contentType(MediaType.APPLICATION_JSON)
                .content("{\"topic\":\"requests.events\"}"))
                .andExpect(status().isBadRequest());
    }

    @Test
    void consultaElClúster() throws Exception {
        when(cluster.describe()).thenReturn(new ClusterResponse("abc", 1, List.of()));

        mvc.perform(get("/api/kafka/cluster"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.clusterId").value("abc"));
    }
}
