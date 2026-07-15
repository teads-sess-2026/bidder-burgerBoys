package com.teads.summerschool;

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;
import org.springframework.data.redis.core.ReactiveRedisTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

// Real Postgres via Testcontainers, not H2 — the schema-per-bidder search_path mechanism
// (see R2dbcConfig) is Postgres-specific (libpq wire-protocol startup options), so testing
// against H2 would validate nothing about the actual multi-tenant-schema design.
@Testcontainers
@SpringBootTest
class SummerschoolApplicationTests {

    @Container
    static final PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:16");

    @DynamicPropertySource
    static void r2dbcProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.r2dbc.url", () -> "r2dbc:postgresql://%s:%d/%s".formatted(
                postgres.getHost(), postgres.getFirstMappedPort(), postgres.getDatabaseName()));
        registry.add("spring.r2dbc.username", postgres::getUsername);
        registry.add("spring.r2dbc.password", postgres::getPassword);
    }

    @TestConfiguration
    static class RedisMockConfig {

        // Mocks ReactiveRedisTemplate rather than hitting a real Redis. Plain deep-stubbing (as
        // the old StringRedisTemplate mock did for its void methods) would hang CreativeSeeder's
        // startup blockLast(): a mocked Mono/Flux never emits onComplete on its own, since
        // ApplicationRunners execute during context startup, before any test method could stub
        // per-call behavior. This bean's answer instead completes empty for any Mono/Flux-
        // returning method, so the ApplicationRunner chain finishes normally.
        @Bean
        @Primary
        ReactiveRedisTemplate<String, String> testReactiveRedisTemplate() {
            return Mockito.mock(ReactiveRedisTemplate.class,
                    (Answer<Object>) RedisMockConfig::emptyReactiveOrDeepStub);
        }

        // Recurses into nested interface return types (e.g. opsForValue()) applying this same
        // answer, so calls like opsForValue().set(...) resolve to Mono.empty() too — a single
        // RETURNS_DEEP_STUBS fallback wouldn't know about the Mono/Flux special-casing once
        // it generates its own sub-mocks.
        private static Object emptyReactiveOrDeepStub(InvocationOnMock invocation) throws Throwable {
            if (invocation.getMethod().getDeclaringClass() == Object.class) {
                return Mockito.RETURNS_DEFAULTS.answer(invocation);
            }
            Class<?> returnType = invocation.getMethod().getReturnType();
            if (returnType == Mono.class) return Mono.empty();
            if (returnType == Flux.class) return Flux.empty();
            if (returnType.isInterface()) {
                return Mockito.mock(returnType, (Answer<Object>) RedisMockConfig::emptyReactiveOrDeepStub);
            }
            return Mockito.RETURNS_DEFAULTS.answer(invocation);
        }
    }

    @Test
    void contextLoads() {
    }

}
