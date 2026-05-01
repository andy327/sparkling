package com.sparkling.row.codec

import shapeless.{::, Generic, HList, HNil}

/** Typeclass that declares whether a value `A` is "present" (i.e., non-null and usable).
  *
  * Downstream code that relies on the presence of usable values (e.g., where a value's absence invalidates a Row from
  * successfully comparing against other Rows) should verify presence to avoid computation on incomplete inputs.
  * Usable values can mean non-null, non-empty, non-NaN, etc.
  */
trait FieldPresence[A] extends Serializable {

  /** Returns true when the `A` value is well-defined and usable. */
  def isPresent(a: A): Boolean
}

object FieldPresence {

  /** Build a [[FieldPresence]] instance from a predicate. */
  def apply[A](f: A => Boolean): FieldPresence[A] = (a: A) => f(a)

  // primitives + generic derivation

  implicit val presenceString: FieldPresence[String] = Option(_).map(_.trim).filter(_.nonEmpty).isDefined
  implicit val presenceInt: FieldPresence[Int] = _ => true
  implicit val presenceLong: FieldPresence[Long] = _ => true
  implicit val presenceDouble: FieldPresence[Double] = !java.lang.Double.isNaN(_)

  implicit val presenceHNil: FieldPresence[HNil] = (_: HNil) => true

  implicit def presenceHCons[H, T <: HList](implicit
      ch: FieldPresence[H],
      ct: FieldPresence[T]
  ): FieldPresence[H :: T] = { case (h :: t) => ch.isPresent(h) && ct.isPresent(t) }

  implicit def presenceProduct[A, L <: HList](implicit
      gen: Generic.Aux[A, L],
      cl: FieldPresence[L]
  ): FieldPresence[A] =
    (a: A) => cl.isPresent(gen.to(a))
}
