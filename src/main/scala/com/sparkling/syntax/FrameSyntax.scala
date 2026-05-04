package com.sparkling.syntax

import org.apache.spark.sql.DataFrame

import com.sparkling.frame.Frame

/** Syntax extension that lifts a Spark `DataFrame` into the sparkling DSL.
  *
  * Adds the `.frame` method to `DataFrame`, returning a [[com.sparkling.frame.Frame]] wrapper that provides the
  * fluent, field-oriented transformation API.
  *
  * Example — filter, group, aggregate, and sort in a single pipeline:
  * {{{
  * import com.sparkling.dsl._
  *
  * val result = df.frame
  *   .filter("score") { score: Int => score > 0 }
  *   .groupBy("dept") {
  *     _.avg("salary" -> "avg_salary")
  *      .count("headcount")
  *   }
  *   .orderBy("dept")
  *   .df
  * }}}
  */
trait FrameSyntax {
  implicit final class DataFrameFrameOps(private val df: DataFrame) {
    def frame: Frame = Frame(df)
  }
}

object FrameSyntax extends FrameSyntax
