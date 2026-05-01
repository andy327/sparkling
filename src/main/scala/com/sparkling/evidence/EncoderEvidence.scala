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

import scala.annotation.unused
import scala.reflect.ClassTag
import scala.reflect.runtime.universe.TypeTag

import org.slf4j.LoggerFactory

import org.apache.spark.ml.linalg.Vector
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.{Encoder, Encoders}
import shapeless.labelled.FieldType
import shapeless.{::, HList, HNil, LabelledGeneric}

/** Evidence that Spark can encode/decode values of type `T`.
  *
  * Spark supports two broad encoding modes:
  *
  *   - Expression encoders: values are representable in Spark SQL's internal row format and can participate in SQL
  *     expressions, Catalyst optimization, and typed aggregators.
  *
  *   - Kryo encoders: values are serialized as opaque bytes and are not SQL-visible, but allow arbitrary Scala types to
  *     be used in Datasets.
  *
  * `EncoderEvidence` makes that choice explicit and composable:
  *
  *   - Prefer ExpressionEncoder when available
  *   - Fall back to Kryo when necessary
  *   - Propagate encoding behavior through tuples, collections, and products
  *
  * This keeps encoder decisions out of call-sites and prevents Spark's encoding rules from leaking into user-facing
  * APIs.
  */
trait EncoderEvidence[T] extends Serializable {

  /** The Spark encoder to use for values of `T`. */
  def encoder: Encoder[T]

  /** A small ADT describing how we chose to encode `T` (primarily for debugging and tests). */
  def strategy: EncodingStrategy
}

/** Stronger evidence that `T` is representable via a Spark SQL ExpressionEncoder.
  *
  * If an implicit `ExprEncoderEvidence[T]` exists, values of `T` can safely be treated as Spark SQL-compatible (e.g.
  * part of a struct, nested inside expression encoders, usable with typed aggregators). If only `EncoderEvidence[T]`
  * exists, Spark can still serialize `T` (typically via Kryo) but it is not SQL-native.
  */
trait ExprEncoderEvidence[T] extends EncoderEvidence[T]

/** Low-priority implicit instances providing the Kryo fallback.
  *
  * Scala implicit search prefers implicits defined in higher-priority scopes. By placing the Kryo fallback here, it is
  * only selected when no `ExprEncoderEvidence[T]` exists.
  */
private[evidence] trait LowPriorityEncoderEvidence {

  implicit def kryo[T](implicit tt: TypeTag[T]): EncoderEvidence[T] = {
    LoggerFactory.getLogger(EncoderEvidence.getClass).debug("falling back to Kryo encoder for {}", tt.tpe)

    implicit val ct: ClassTag[T] = ClassTag[T](tt.mirror.runtimeClass(tt.tpe))

    new EncoderEvidence[T] {
      override val encoder: Encoder[T] = Encoders.kryo[T]
      override val strategy: EncodingStrategy =
        EncodingStrategy.Kryo(
          tpe = tt.tpe.toString,
          runtimeClass = ct.runtimeClass.getName
        )
    }
  }
}

object EncoderEvidence extends BoilerplateEncoderEvidence with LowPriorityEncoderEvidence {

  private def forExpr[T: TypeTag]: ExprEncoderEvidence[T] =
    new ExprEncoderEvidence[T] {
      override val encoder: Encoder[T] = ExpressionEncoder()
      override val strategy: EncodingStrategy =
        EncodingStrategy.Expr(implicitly[TypeTag[T]].tpe.toString)
    }

  implicit val forInt: ExprEncoderEvidence[Int] = forExpr[Int]
  implicit val forLong: ExprEncoderEvidence[Long] = forExpr[Long]
  implicit val forDouble: ExprEncoderEvidence[Double] = forExpr[Double]
  implicit val forFloat: ExprEncoderEvidence[Float] = forExpr[Float]
  implicit val forByte: ExprEncoderEvidence[Byte] = forExpr[Byte]
  implicit val forShort: ExprEncoderEvidence[Short] = forExpr[Short]
  implicit val forBoolean: ExprEncoderEvidence[Boolean] = forExpr[Boolean]
  implicit val forString: ExprEncoderEvidence[String] = forExpr[String]

  implicit val forJInt: ExprEncoderEvidence[JInt] = forExpr[JInt]
  implicit val forJLong: ExprEncoderEvidence[JLong] = forExpr[JLong]
  implicit val forJDouble: ExprEncoderEvidence[JDouble] = forExpr[JDouble]
  implicit val forJFloat: ExprEncoderEvidence[JFloat] = forExpr[JFloat]
  implicit val forJByte: ExprEncoderEvidence[JByte] = forExpr[JByte]
  implicit val forJShort: ExprEncoderEvidence[JShort] = forExpr[JShort]
  implicit val forJBoolean: ExprEncoderEvidence[JBoolean] = forExpr[JBoolean]

  implicit val forBigDecimal: ExprEncoderEvidence[BigDecimal] = forExpr[BigDecimal]
  implicit val forDate: ExprEncoderEvidence[Date] = forExpr[Date]
  implicit val forVector: ExprEncoderEvidence[Vector] = forExpr[Vector]

  implicit def forSeq[T](implicit
      @unused ev: ExprEncoderEvidence[T],
      tt: TypeTag[Seq[T]]
  ): ExprEncoderEvidence[Seq[T]] =
    forExpr[Seq[T]]

  implicit def forArray[T](implicit
      @unused ev: ExprEncoderEvidence[T],
      tt: TypeTag[Array[T]]
  ): ExprEncoderEvidence[Array[T]] =
    forExpr[Array[T]]

  implicit def forOption[T](implicit
      @unused ev: ExprEncoderEvidence[T],
      tt: TypeTag[Option[T]]
  ): ExprEncoderEvidence[Option[T]] =
    forExpr[Option[T]]

  implicit def forMap[K, V](implicit
      @unused kev: ExprEncoderEvidence[K],
      @unused vev: ExprEncoderEvidence[V],
      tt: TypeTag[Map[K, V]]
  ): ExprEncoderEvidence[Map[K, V]] =
    forExpr[Map[K, V]]

  trait HListExprEncoderConstraint[R]
  object HListExprEncoderConstraint {
    implicit val hnil: HListExprEncoderConstraint[HNil] = new HListExprEncoderConstraint[HNil] {}

    implicit def hcons[HeadName, HeadVal, Tail <: HList](implicit
        @unused head: ExprEncoderEvidence[HeadVal],
        @unused tail: HListExprEncoderConstraint[Tail]
    ): HListExprEncoderConstraint[FieldType[HeadName, HeadVal] :: Tail] =
      new HListExprEncoderConstraint[FieldType[HeadName, HeadVal] :: Tail] {}
  }

  implicit def forProduct[T <: Product, R](implicit
      @unused gen: LabelledGeneric.Aux[T, R],
      @unused ok: HListExprEncoderConstraint[R],
      tt: TypeTag[T]
  ): ExprEncoderEvidence[T] =
    forExpr[T]
}
