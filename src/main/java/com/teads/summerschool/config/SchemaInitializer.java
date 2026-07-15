package com.teads.summerschool.config;

/**
 * Derives this bidder's isolated Postgres schema name from its bidder id.
 * Used by {@link R2dbcConfig} (to scope every connection's search_path) and
 * {@link SchemaBootstrap} (to create the schema/tables on startup).
 */
public final class SchemaInitializer {

    private SchemaInitializer() {
    }

    /** Derive a safe Postgres schema name from a bidder id. Only [a-z0-9_] survives. */
    public static String schemaFor(String bidderId) {
        return "bidder_" + bidderId.toLowerCase().replaceAll("[^a-z0-9]+", "_");
    }
}
