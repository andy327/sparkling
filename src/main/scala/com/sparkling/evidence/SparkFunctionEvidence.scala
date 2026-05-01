package com.sparkling.evidence

import scala.reflect.runtime.universe.TypeTag

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.{functions => sqlf}

/** Evidence that a Scala function `F` can be turned into a Spark `UserDefinedFunction`.
  *
  * Covers arities `Function0` through `Function10` via boilerplate instances.
  *
  * The function's return type is tracked as the abstract type member [[Elem]]:
  *   - [[udf]] produces a UDF returning `Elem`
  *   - [[udfExplode]] produces a UDF returning `Seq[A]` by first converting `Elem` via [[ExplodeEvidence]]
  *
  * The `Aux` type alias is a standard pattern for naming the type member when you need to pass `R` to another
  * typeclass or make it visible to the compiler.
  */
trait SparkFunctionEvidence[F] extends Serializable {
  type Elem
  def udf(f: F): UserDefinedFunction
  def udfExplode[A: TypeTag](f: F)(implicit ee: ExplodeEvidence.Aux[Elem, A]): UserDefinedFunction
}

object SparkFunctionEvidence extends BoilerplateSparkFunctionEvidence {
  type Aux[F, R] = SparkFunctionEvidence[F] { type Elem = R }

  implicit def forFunction0[R: TypeTag]: Aux[() => R, R] =
    new SparkFunctionEvidence[() => R] {
      type Elem = R
      def udf(f: () => R): UserDefinedFunction = sqlf.udf(f)
      def udfExplode[A: TypeTag](f: () => R)(implicit ee: ExplodeEvidence.Aux[R, A]): UserDefinedFunction =
        sqlf.udf(() => ee.toSeq(f()))
    }
}
