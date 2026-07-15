package com.teads.summerschool.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.ReactiveRedisConnectionFactory;
import org.springframework.data.redis.core.ReactiveRedisTemplate;
import org.springframework.data.redis.serializer.RedisSerializationContext;

/**
 * Boot autoconfigures a default ReactiveRedisTemplate<Object,Object> alongside the blocking
 * StringRedisTemplate whenever Lettuce is on the classpath (it already is here). This bean
 * gives BidderStatsCache the same plain string key/value semantics StringRedisTemplate had.
 */
@Configuration
public class RedisConfig {

    @Bean
    public ReactiveRedisTemplate<String, String> reactiveStringRedisTemplate(ReactiveRedisConnectionFactory factory) {
        return new ReactiveRedisTemplate<>(factory, RedisSerializationContext.string());
    }

    /**
     * ObjectMapper for JSON serialization of Creative objects stored in Redis.
     * Configured to handle Java 8+ types and ignore null values for compact storage.
     */
    @Bean
    public ObjectMapper objectMapper() {
        ObjectMapper mapper = new ObjectMapper();
        mapper.findAndRegisterModules(); // Register Java 8 time module, etc.
        mapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS); // ISO-8601 format
        return mapper;
    }
}
