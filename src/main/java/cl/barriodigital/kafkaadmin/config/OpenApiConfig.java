package cl.barriodigital.kafkaadmin.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import io.swagger.v3.oas.models.Components;
import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.info.Info;
import io.swagger.v3.oas.models.security.SecurityRequirement;
import io.swagger.v3.oas.models.security.SecurityScheme;

/** Metadatos de la documentación OpenAPI y el esquema Bearer JWT que usa toda la API. */
@Configuration
public class OpenApiConfig {

    public static final String BEARER_SCHEME = "bearerAuth";

    @Bean
    OpenAPI kafkaAdminOpenApi() {
        return new OpenAPI()
                .info(new Info()
                        .title("BarrioDigital - Administrador de Kafka")
                        .version("1.0.0")
                        .description("Administra tópicos (crear, consultar, configurar, ampliar particiones y eliminar) y consumer groups (lag, offsets). "
                                + "Requiere un JWT de Entra ID con el rol Admin (401 sin token, 403 con otro rol). "
                                + "Los tópicos del sistema (requests.events, audit.timeline) están protegidos contra el borrado."))
                .components(new Components().addSecuritySchemes(BEARER_SCHEME, new SecurityScheme()
                        .type(SecurityScheme.Type.HTTP)
                        .scheme("bearer")
                        .bearerFormat("JWT")))
                .addSecurityItem(new SecurityRequirement().addList(BEARER_SCHEME));
    }
}
