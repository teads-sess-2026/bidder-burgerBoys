package com.teads.summerschool.config;

import io.r2dbc.postgresql.PostgresqlConnectionFactoryProvider;
import org.springframework.boot.r2dbc.autoconfigure.ConnectionFactoryOptionsBuilderCustomizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Injects this bidder's schema into every R2DBC connection via the driver's SCHEMA option (sets
 * Postgres's search_path at connection time) — the R2DBC equivalent of Hibernate's
 * hibernate.default_schema, which had no direct analog once JPA/Hikari were removed. Boot's own
 * R2dbcAutoConfiguration (URL parsing, pooling via spring.r2dbc.pool.*, credentials) picks up
 * this customizer automatically, so there's no need to rebuild any of that by hand.
 *
 * <p>One JVM = one bidder = one schema for the whole process lifetime, so the search_path option
 * is static per pooled connection — no per-checkout reset is needed.
 */
@Configuration
public class R2dbcConfig {

    @Bean
    public ConnectionFactoryOptionsBuilderCustomizer searchPathCustomizer(BidderProperties bidderProperties) {
        String schema = SchemaInitializer.schemaFor(bidderProperties.getId());
        return builder -> builder.option(PostgresqlConnectionFactoryProvider.SCHEMA, schema);
    }
}
