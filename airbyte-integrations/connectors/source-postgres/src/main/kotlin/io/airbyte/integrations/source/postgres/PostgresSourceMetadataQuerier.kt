/*
 * Copyright (c) 2025 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.source.postgres

import io.airbyte.cdk.ConfigErrorException
import io.airbyte.cdk.StreamIdentifier
import io.airbyte.cdk.check.JdbcCheckQueries
import io.airbyte.cdk.command.FeatureFlag
import io.airbyte.cdk.discover.EmittedField
import io.airbyte.cdk.discover.JdbcMetadataQuerier
import io.airbyte.cdk.discover.MetadataQuerier
import io.airbyte.cdk.discover.TableName
import io.airbyte.cdk.jdbc.ArrayFieldType
import io.airbyte.cdk.jdbc.DefaultJdbcConstants
import io.airbyte.cdk.jdbc.JdbcConnectionFactory
import io.airbyte.cdk.jdbc.JdbcFieldType
import io.airbyte.cdk.read.SelectQueryGenerator
import io.airbyte.cdk.ssh.SshNoTunnelMethod
import io.airbyte.integrations.source.postgres.config.PostgresSourceConfiguration
import io.airbyte.integrations.source.postgres.config.XminIncrementalConfiguration
import io.airbyte.protocol.models.v0.ConfiguredAirbyteCatalog
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micronaut.context.annotation.Primary
import jakarta.inject.Singleton
import java.sql.Connection

class PostgresSourceMetadataQuerier(
    val base: JdbcMetadataQuerier,
    val postgresSourceConfig: PostgresSourceConfiguration,
    private val featureFlags: Set<FeatureFlag>,
) : MetadataQuerier by base {

    override fun extraChecks() {
        base.extraChecks()
        validateSslConfiguration()
        if (postgresSourceConfig.incrementalConfiguration is XminIncrementalConfiguration) {
            base.conn.use { conn ->
                if (dbNumWraparound(conn) > 0) {
                    throw ConfigErrorException(xminWraparoundError)
                }
            }
        }
    }

    /**
     * Postgres collapses `T[]` and `T[][]` (and deeper) into the same catalog type, so the
     * generic [JdbcMetadataQuerier] always reports array columns as a single level of nesting.
     * Here we consult `pg_attribute.attndims` - the one place Postgres records the *declared*
     * dimensionality - and re-wrap the field's [ArrayFieldType] to match, so that genuinely
     * multi-dimensional array columns (e.g. `double precision[][]`) round-trip correctly instead
     * of being flattened and subsequently nulled out downstream.
     *
     * Note: `attndims` is a hint, not an enforced constraint - Postgres permits storing arrays of
     * a different depth than declared. This only corrects the common case where the stored data
     * matches the declared shape.
     */
    override fun fields(streamID: StreamIdentifier): List<EmittedField> {
        val emittedFields = base.fields(streamID)
        val table = base.findTableName(streamID) ?: return emittedFields
        val attndimsByColumn = fetchAttndims(table)

        return emittedFields.map { field ->
            val dims = attndimsByColumn[field.id] ?: return@map field
            if (dims <= 1) return@map field
            val type = field.type
            if (type !is ArrayFieldType<*>) return@map field
            var nested: JdbcFieldType<*> = type
            repeat(dims - 1) { nested = ArrayFieldType(nested) }
            field.copy(type = nested)
        }
    }

    private fun fetchAttndims(table: TableName): Map<String, Int> {
        val query =
            """
            SELECT a.attname, a.attndims
            FROM pg_catalog.pg_attribute a
            JOIN pg_catalog.pg_class c ON a.attrelid = c.oid
            JOIN pg_catalog.pg_namespace n ON c.relnamespace = n.oid
            WHERE n.nspname = ? AND c.relname = ? AND a.attnum > 0 AND NOT a.attisdropped
            """.trimIndent()
        base.conn.prepareStatement(query).use { stmt ->
            stmt.setString(1, table.schema)
            stmt.setString(2, table.name)
            stmt.executeQuery().use { rs ->
                val result = mutableMapOf<String, Int>()
                while (rs.next()) {
                    result[rs.getString("attname")] = rs.getInt("attndims")
                }
                return result
            }
        }
    }

    /**
     * Validates that the SSL/SSH configuration is sufficient for Airbyte Cloud. On Cloud, we
     * require SSL encryption or an SSH tunnel; connections with ssl_mode in [disable, allow,
     * prefer] and no tunnel are rejected.
     */
    private fun validateSslConfiguration() {
        if (!featureFlags.contains(FeatureFlag.AIRBYTE_CLOUD_DEPLOYMENT)) {
            return
        }
        val sslMode: String? = postgresSourceConfig.jdbcProperties["sslmode"]
        val acceptableSslModes = listOf("require", "verify-ca", "verify-full")
        val hasNoTunnel = postgresSourceConfig.sshTunnel is SshNoTunnelMethod
        if (sslMode !in acceptableSslModes && hasNoTunnel) {
            throw ConfigErrorException(
                "Connection from Airbyte Cloud requires SSL encryption or an SSH tunnel. " +
                    "Current SSL mode: $sslMode",
            )
        }
    }

    companion object {
        private val log = KotlinLogging.logger {}

        const val xminWraparoundError: String =
            "We detected XMIN transaction wraparound in the database, " +
                "which makes this sync option inefficient and can lead to higher credit consumption. " +
                "Please change the replication method to CDC or cursor based."

        public fun dbNumWraparound(conn: Connection): Long {
            log.info { "Querying server xmin wraparound status" }
            val query =
                """
            select (txid_snapshot_xmin(txid_current_snapshot()) >> 32) AS num_wraparound
        """.trimIndent()
            conn.createStatement().use { stmt ->
                stmt.executeQuery(query).use { rs ->
                    if (rs.next()) {
                        val numWraparound = rs.getLong("num_wraparound")
                        return numWraparound
                    }
                }
            }
            return 0
        }
    }
}

@Singleton
@Primary
class Factory(
    val constants: DefaultJdbcConstants,
    val selectQueryGenerator: SelectQueryGenerator,
    val fieldTypeMapper: JdbcMetadataQuerier.FieldTypeMapper,
    val checkQueries: JdbcCheckQueries,
    val featureFlags: Set<FeatureFlag>,
    val configuredCatalog: ConfiguredAirbyteCatalog? = null,
) : MetadataQuerier.Factory<PostgresSourceConfiguration> {
    override fun session(config: PostgresSourceConfiguration): MetadataQuerier {
        val jdbcConnectionFactory = JdbcConnectionFactory(config)
        val base =
            JdbcMetadataQuerier(
                constants,
                config,
                selectQueryGenerator,
                fieldTypeMapper,
                checkQueries,
                jdbcConnectionFactory,
            )
        return PostgresSourceMetadataQuerier(base, config, featureFlags)
    }
}
