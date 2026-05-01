package com.sparkling.evidence

/** Describes how a Spark `org.apache.spark.sql.Encoder` was selected/constructed.
  *
  * Intended for debugging and for precise unit tests.
  */
sealed trait EncodingStrategy extends Product with Serializable {
  final def render: String = EncodingStrategy.render(this)
}

object EncodingStrategy {

  /** Spark SQL expression-backed encoder (row-compatible) */
  final case class Expr(tpe: String) extends EncodingStrategy

  /** Kryo-based (binary) encoder fallback */
  final case class Kryo(tpe: String, runtimeClass: String) extends EncodingStrategy

  /** A struct/tuple encoder built from element encoders (some elements may be Kryo/binary). */
  final case class Struct(elems: Vector[EncodingStrategy]) extends EncodingStrategy

  def render(s: EncodingStrategy): String = s match {
    case Expr(tpe)               => s"expr<$tpe>"
    case Kryo(tpe, runtimeClass) => s"kryo<$tpe;$runtimeClass>"
    case Struct(elems)           => s"struct<(${elems.iterator.map(render).mkString(",")})>"
  }
}
