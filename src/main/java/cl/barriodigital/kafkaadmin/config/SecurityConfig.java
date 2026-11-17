package cl.barriodigital.kafkaadmin.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.config.annotation.method.configuration.EnableMethodSecurity;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.config.http.SessionCreationPolicy;
import org.springframework.security.oauth2.server.resource.authentication.JwtAuthenticationConverter;
import org.springframework.security.oauth2.server.resource.authentication.JwtGrantedAuthoritiesConverter;
import org.springframework.security.web.SecurityFilterChain;

import cl.barriodigital.kafkaadmin.security.RestSecurityErrorHandler;

/**
 * Misma configuración de resource server que rabbitadmin (JWT de Entra ID, roles en el claim
 * {@code roles}): todo lo que no sea el health o la documentación OpenAPI exige el rol Admin.
 *
 * <p>QUÉ ES: la seguridad del servicio. Valida el JWT de Azure y exige el rol "Admin" en TODA la API
 * (a diferencia de requests/catalog, aquí no se distribuyen permisos por método).
 */
@Configuration
@EnableWebSecurity
@EnableMethodSecurity
public class SecurityConfig {

    public static final String ADMIN_ROLE = "Admin";

    @Bean
    SecurityFilterChain filterChain(HttpSecurity http, JwtAuthenticationConverter jwtAuthenticationConverter,
                                    RestSecurityErrorHandler errorHandler) throws Exception {
        http
                // API con token y sin sesión: CSRF no aplica; el servidor no guarda estado (STATELESS).
                .csrf(csrf -> csrf.disable())
                .sessionManagement(session -> session.sessionCreationPolicy(SessionCreationPolicy.STATELESS))
                .authorizeHttpRequests(auth -> auth
                        // El health es público: Docker y los chequeos de despliegue lo consultan sin token.
                        .requestMatchers("/actuator/health", "/actuator/health/**").permitAll()
                        // Documentación OpenAPI: pública en el servicio (el puerto solo se publica en localhost).
                        .requestMatchers("/v3/api-docs", "/v3/api-docs/**", "/swagger-ui.html", "/swagger-ui/**").permitAll()
                        // Todo lo demás: solo con el rol Admin.
                        .anyRequest().hasAuthority(ADMIN_ROLE))
                // 401 (sin token) y 403 (rol insuficiente) se responden como JSON con el formato común de la API.
                .exceptionHandling(ex -> ex
                        .authenticationEntryPoint(errorHandler)
                        .accessDeniedHandler(errorHandler))
                // Valida el JWT (firma, emisor, expiración) y convierte el claim "roles" en permisos.
                .oauth2ResourceServer(oauth2 -> oauth2
                        .authenticationEntryPoint(errorHandler)
                        .accessDeniedHandler(errorHandler)
                        .jwt(jwt -> jwt.jwtAuthenticationConverter(jwtAuthenticationConverter)));
        return http.build();
    }

    // Lee los roles del claim "roles" (así los entrega Azure) y los usa sin prefijo.
    @Bean
    JwtAuthenticationConverter jwtAuthenticationConverter() {
        JwtGrantedAuthoritiesConverter authorities = new JwtGrantedAuthoritiesConverter();
        authorities.setAuthoritiesClaimName("roles");
        authorities.setAuthorityPrefix(""); // 'Admin', 'Operador', 'Cliente', 'Auditor' tal cual vienen en el token

        JwtAuthenticationConverter converter = new JwtAuthenticationConverter();
        converter.setJwtGrantedAuthoritiesConverter(authorities);
        return converter;
    }
}
