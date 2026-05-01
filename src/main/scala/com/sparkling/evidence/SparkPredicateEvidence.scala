package com.sparkling.evidence

import org.apache.spark.sql.expressions.UserDefinedFunction

/** Evidence that a Scala predicate function `F` can be turned into a Spark `UserDefinedFunction`.
  *
  * A specialization of [[SparkFunctionEvidence]] for boolean-valued functions. Only functions returning `Boolean` are
  * supported. Function arities start at 1 (Function1..Function10). Zero-argument predicates are intentionally excluded
  * since Spark UDF predicates must consume at least one column.
  */
trait SparkPredicateEvidence[F] extends Serializable {

  /** Convert the Scala predicate function into a Spark `UserDefinedFunction`. */
  def udf(f: F): UserDefinedFunction
}

object SparkPredicateEvidence extends BoilerplateSparkPredicateEvidence
