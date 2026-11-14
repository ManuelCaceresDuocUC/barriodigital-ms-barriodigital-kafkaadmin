package cl.barriodigital.kafkaadmin.service;

import org.springframework.stereotype.Component;
import org.springframework.util.PatternMatchUtils;

import cl.barriodigital.kafkaadmin.config.ProtectionProperties;

/** Decide qué tópicos pertenecen al sistema y no pueden eliminarse. */
@Component
public class ProtectedTopics {

    private final String[] patterns;

    public ProtectedTopics(ProtectionProperties properties) {
        this.patterns = properties.topics().toArray(String[]::new);
    }

    public boolean isProtected(String name) {
        return name != null && PatternMatchUtils.simpleMatch(patterns, name);
    }
}
