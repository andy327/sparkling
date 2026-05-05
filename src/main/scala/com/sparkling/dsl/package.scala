package com.sparkling

/** Single-import entry point for the sparkling DSL.
  *
  * Brings into scope:
  *   - String / tuple → [[com.sparkling.schema.Field]] / [[com.sparkling.schema.Fields]] implicit conversions, so
  *     frame methods can be called with plain strings rather than explicit `Field`/`Fields` constructors.
  *   - `df.frame` extension method for lifting a Spark `DataFrame` into [[com.sparkling.frame.Frame]].
  *   - `withDeterminism` / `withNullable` extension methods on Spark `UserDefinedFunction`
  *     ([[com.sparkling.syntax.UserDefinedFunctionSyntax]]).
  *   - Algebird aggregation adapters ([[com.sparkling.algebird.AlgebirdAggregatorSyntax]]) and enrichment
  *     ([[com.sparkling.algebird.AlgebirdSyntax]]).
  *
  * Usage:
  * {{{
  * import com.sparkling.dsl._
  * }}}
  */
package object dsl
    extends syntax.FieldsSyntax
    with syntax.FrameSyntax
    with syntax.UserDefinedFunctionSyntax
    with algebird.AlgebirdAggregatorSyntax
    with algebird.AlgebirdSyntax
