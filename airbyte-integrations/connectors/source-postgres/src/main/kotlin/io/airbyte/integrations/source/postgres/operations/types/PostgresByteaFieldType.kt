/* Copyright (c) 2026 Airbyte, Inc., all rights reserved. */
package io.airbyte.integrations.source.postgres.operations.types

import io.airbyte.cdk.data.LeafAirbyteSchemaType
import io.airbyte.cdk.data.TextCodec
import io.airbyte.cdk.jdbc.JdbcAccessor
import io.airbyte.cdk.jdbc.SymmetricJdbcFieldType
import java.sql.PreparedStatement
import java.sql.ResultSet
import java.util.Base64
import java.util.HexFormat

object PostgresByteaFieldType :
    SymmetricJdbcFieldType<String>(LeafAirbyteSchemaType.STRING, PgByteaAccessor, TextCodec)

private object PgByteaAccessor : JdbcAccessor<String> {
    override fun get(rs: ResultSet, colIdx: Int): String? {
        val bytes = rs.getBytes(colIdx)?.takeUnless { rs.wasNull() } ?: return null
        return Base64.getEncoder().encodeToString(bytes)
    }

    override fun set(stmt: PreparedStatement, paramIdx: Int, value: String) {
        stmt.setBytes(paramIdx, Base64.getDecoder().decode(value))
    }
}

/**
 * Scalar `bytea` columns are encoded as plain hex (no `\x` prefix), equivalent to
 * `encode(col, 'hex')` - unlike the legacy [PostgresByteaFieldType] base64 encoding used for
 * `bytea[]` array columns.
 */
object PostgresByteaHexFieldType :
    SymmetricJdbcFieldType<String>(LeafAirbyteSchemaType.STRING, PgByteaHexAccessor, TextCodec)

private object PgByteaHexAccessor : JdbcAccessor<String> {
    override fun get(rs: ResultSet, colIdx: Int): String? {
        val bytes = rs.getBytes(colIdx)?.takeUnless { rs.wasNull() } ?: return null
        return HexFormat.of().formatHex(bytes)
    }

    override fun set(stmt: PreparedStatement, paramIdx: Int, value: String) {
        stmt.setBytes(paramIdx, HexFormat.of().parseHex(value))
    }
}
