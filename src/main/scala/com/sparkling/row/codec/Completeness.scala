package com.sparkling.row.codec

import shapeless.{::, Generic, HList, HNil}

/** Typeclass that declares whether a value `A` is "complete" (i.e., checking for null/unusable values).
  *
  * Downstream code that relies on the presence of usable values (e.g., where a value's absence invalidates a Row from
  * successfully comparing against other Rows) should verify completeness to avoid computation on incomplete inputs.
  * Usable values can mean non-null, non-empty, non-NaN, etc.
  */
trait Completeness[A] extends Serializable {

  /** Returns true when the `A` value is well-defined and usable. */
  def isComplete(a: A): Boolean
}

object Completeness {

  /** Build a [[Completeness]] instance from a predicate. */
  def apply[A](f: A => Boolean): Completeness[A] = (a: A) => f(a)

  // primitives + generic derivation

  implicit val completenessString: Completeness[String] = Option(_).map(_.trim).filter(_.nonEmpty).isDefined
  implicit val completenessInt: Completeness[Int] = _ => true
  implicit val completenessLong: Completeness[Long] = _ => true
  implicit val completenessDouble: Completeness[Double] = !java.lang.Double.isNaN(_)

  implicit val completenessHNil: Completeness[HNil] = (_: HNil) => true

  implicit def completenessHCons[H, T <: HList](implicit
      ch: Completeness[H],
      ct: Completeness[T]
  ): Completeness[H :: T] = { case (h :: t) => ch.isComplete(h) && ct.isComplete(t) }

  implicit def completenessProduct[A, L <: HList](implicit
      gen: Generic.Aux[A, L],
      cl: Completeness[L]
  ): Completeness[A] =
    (a: A) => cl.isComplete(gen.to(a))
}
