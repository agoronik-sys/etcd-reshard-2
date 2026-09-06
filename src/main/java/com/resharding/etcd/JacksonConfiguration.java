package com.resharding.etcd;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * {@link ObjectMapper} для сериализации состояния в {@code etcd}.
 *
 * <p>{@code FAIL_ON_UNKNOWN_PROPERTIES} отключён: состояние, записанное
 * предыдущей версией сервиса, должно читаться после rolling update,
 * даже если модель получила новые поля.
 */
@Configuration
public class JacksonConfiguration {

    @Bean
    public ObjectMapper objectMapper() {
        ObjectMapper mapper = new ObjectMapper();
        mapper.registerModule(new JavaTimeModule());
        mapper.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
        return mapper;
    }
}
