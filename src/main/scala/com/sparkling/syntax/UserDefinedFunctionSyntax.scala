package com.sparkling.syntax

import org.apache.spark.sql.expressions.UserDefinedFunction

trait UserDefinedFunctionSyntax {

  implicit final class UserDefinedFunctionOps(private val udf: UserDefinedFunction) {

    /** Optionally marks this UDF as non-deterministic.
      *
      * When `deterministic = false`, delegates to Spark's `asNondeterministic()`. When `deterministic = true`, the UDF
      * is returned unchanged. Note: Spark UDF determinism is monotonic — once marked non-deterministic it cannot be
      * made deterministic again.
      */
    def withDeterminism(deterministic: Boolean): UserDefinedFunction =
      if (deterministic) udf else udf.asNondeterministic()

    /** Optionally marks this UDF as non-nullable.
      *
      * When `nullable = false`, delegates to Spark's `asNonNullable()`. When `nullable = true`, the UDF is returned
      * unchanged.
      */
    def withNullable(nullable: Boolean): UserDefinedFunction =
      if (nullable) udf else udf.asNonNullable()
  }
}

object UserDefinedFunctionSyntax extends UserDefinedFunctionSyntax
