package com.sparkling.syntax

import org.apache.spark.sql.DataFrame

import com.sparkling.frame.Frame

/** Syntax extension that lifts a Spark `DataFrame` into the sparkling DSL.
  *
  * Adds the `.frame` method to `DataFrame`, returning a [[com.sparkling.frame.Frame]] wrapper that provides the
  * fluent, field-oriented transformation API.
  *
  * Usage:
  * {{{
  * import com.sparkling.syntax.FrameSyntax._
  *
  * val out = df.frame.repartition(4).orderBy("id").df
  * }}}
  */
trait FrameSyntax {
  implicit final class DataFrameFrameOps(private val df: DataFrame) {
    def frame: Frame = Frame(df)
  }
}

object FrameSyntax extends FrameSyntax
