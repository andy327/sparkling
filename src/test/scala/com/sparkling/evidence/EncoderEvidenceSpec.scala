package com.sparkling.evidence

import java.lang.{
  Boolean => JBoolean,
  Byte => JByte,
  Double => JDouble,
  Float => JFloat,
  Integer => JInt,
  Long => JLong,
  Short => JShort
}
import java.math.BigDecimal
import java.sql.Date

import org.apache.spark.ml.linalg.Vector
import org.apache.spark.sql.types.{BinaryType, StringType, StructField, StructType}
import org.scalatest.matchers.should.Matchers._
import org.scalatest.wordspec.AnyWordSpec

object EncoderEvidenceSpec {

  final case class Person(name: String, age: Int)

  final case class GeoReading(position: (Double, Double), altitude: Option[Double])

  final class PlainObject
}

final class EncoderEvidenceSpec extends AnyWordSpec {
  import EncoderEvidenceSpec._

  private def isExpr[T](implicit ev: EncoderEvidence[T]): Boolean =
    ev.isInstanceOf[ExprEncoderEvidence[T]]

  private def strategy[T](implicit ev: EncoderEvidence[T]): EncodingStrategy =
    ev.strategy

  "EncoderEvidence" should {

    "provide ExprEncoderEvidence for primitive Scala and boxed Java types" in {
      isExpr[Int] shouldBe true
      isExpr[Long] shouldBe true
      isExpr[Double] shouldBe true
      isExpr[Float] shouldBe true
      isExpr[Byte] shouldBe true
      isExpr[Short] shouldBe true
      isExpr[Boolean] shouldBe true

      isExpr[JInt] shouldBe true
      isExpr[JLong] shouldBe true
      isExpr[JDouble] shouldBe true
      isExpr[JFloat] shouldBe true
      isExpr[JByte] shouldBe true
      isExpr[JShort] shouldBe true
      isExpr[JBoolean] shouldBe true

      isExpr[String] shouldBe true

      strategy[Int] shouldBe a[EncodingStrategy.Expr]
      strategy[String] shouldBe a[EncodingStrategy.Expr]
    }

    "provide ExprEncoderEvidence for Option of Expr-encodable values" in {
      isExpr[Option[String]] shouldBe true
      isExpr[Option[Int]] shouldBe true

      strategy[Option[String]] shouldBe a[EncodingStrategy.Expr]
    }

    "provide ExprEncoderEvidence for case classes whose fields are all Expr-encodable" in {
      isExpr[Person] shouldBe true
      isExpr[GeoReading] shouldBe true

      strategy[Person] shouldBe a[EncodingStrategy.Expr]
      strategy[GeoReading] shouldBe a[EncodingStrategy.Expr]
    }

    "fall back to Kryo for non-expression-encodable types" in {
      isExpr[PlainObject] shouldBe false
      strategy[PlainObject] shouldBe a[EncodingStrategy.Kryo]
    }

    "support nested collections when all element types are Expr-encodable" in {
      isExpr[Map[BigDecimal, Seq[(Short, Byte)]]] shouldBe true
      isExpr[Array[Option[(Date, Vector)]]] shouldBe true

      strategy[Map[BigDecimal, Seq[(Short, Byte)]]] shouldBe a[EncodingStrategy.Expr]
      strategy[Array[Option[(Date, Vector)]]] shouldBe a[EncodingStrategy.Expr]
    }

    "provide ExprEncoderEvidence for tuples of Expr-encodable elements" in {
      isExpr[(String, Int)] shouldBe true
      isExpr[(Double, Boolean, Long)] shouldBe true

      strategy[(String, Int)] shouldBe a[EncodingStrategy.Expr]
      strategy[(Double, Boolean, Long)] shouldBe a[EncodingStrategy.Expr]
    }

    "support Tuple4 encoders for Expr-only tuples" in {
      isExpr[(String, Int, Long, Boolean)] shouldBe true
      strategy[(String, Int, Long, Boolean)] shouldBe a[EncodingStrategy.Expr]
    }

    "compose Expr + Kryo encoders for mixed tuples as a struct with binary fields" in {
      val ev = implicitly[EncoderEvidence[(String, PlainObject)]]

      isExpr[(String, PlainObject)] shouldBe false

      ev.strategy match {
        case strat @ EncodingStrategy.Struct(elems) =>
          strat.render should startWith("struct<(")
          elems.length shouldBe 2
          elems.head shouldBe a[EncodingStrategy.Expr]
          elems.head.render should startWith("expr<")
          elems(1) shouldBe a[EncodingStrategy.Kryo]
          elems(1).render should startWith("kryo<")
        case other =>
          fail(s"expected Struct strategy, got: ${other.render}")
      }

      ev.encoder.schema shouldBe StructType(
        Seq(
          StructField("_1", StringType, nullable = true),
          StructField("_2", BinaryType, nullable = true)
        )
      )
    }

    "fall back to Kryo for collections whose element type is not Expr-encodable" in {
      isExpr[Seq[PlainObject]] shouldBe false
      isExpr[Array[PlainObject]] shouldBe false

      strategy[Seq[PlainObject]] shouldBe a[EncodingStrategy.Kryo]
      strategy[Array[PlainObject]] shouldBe a[EncodingStrategy.Kryo]
    }
  }
}
