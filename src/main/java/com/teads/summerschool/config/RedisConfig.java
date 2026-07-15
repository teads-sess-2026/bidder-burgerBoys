package com.teads.summerschool.config;

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
}
