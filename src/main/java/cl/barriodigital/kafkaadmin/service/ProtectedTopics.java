package cl.barriodigital.kafkaadmin.service;

import org.springframework.stereotype.Component;
import org.springframework.util.PatternMatchUtils;

import cl.barriodigital.kafkaadmin.config.ProtectionProperties;

/**
 * QUÉ ES: componente que decide qué tópicos son "del sistema" y no pueden eliminarse.
 * QUÉ HACE: compara el nombre contra los patrones de barriodigital.kafkaadmin.protection.topics (application.yaml).
 */
@Component
public class ProtectedTopics {

    // Patrones configurados (admiten comodín *, por ejemplo "audit.*").
    private final String[] patterns;

    public ProtectedTopics(ProtectionProperties properties) {
        this.patterns = properties.topics().toArray(String[]::new);
    }

    // true si el nombre coincide con algún patrón protegido.
    public boolean isProtected(String name) {
        return name != null && PatternMatchUtils.simpleMatch(patterns, name);
    }
}
