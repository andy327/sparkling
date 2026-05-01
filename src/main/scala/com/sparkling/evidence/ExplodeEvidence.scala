package com.sparkling.evidence

import scala.collection.immutable.ArraySeq

/** Evidence that a value of type `R` can be treated as a collection of elements suitable for Spark `explode`.
  *
  * Accepts functions returning a collection (`Seq[A]`, `Array[A]`) or a "0-or-1" container (`Option[A]`), converting
  * the return value into a null-safe `Seq[Elem]`: a `null` result yields `Seq.empty` so it never crashes at runtime.
  *
  * The element type is captured as an abstract type member `Elem` so it can be threaded through other typeclass
  * constraints without forcing callers to name intermediate types.
  */
trait ExplodeEvidence[R] extends Serializable {
  type Elem
  def toSeq(r: R): Seq[Elem]
}

object ExplodeEvidence {

  /** Aux pattern for referencing the inferred element type `Elem` as a normal type parameter. */
  type Aux[R, A] = ExplodeEvidence[R] { type Elem = A }

  /** Supports any `Seq` subtype (`List`, `Vector`, etc.) via an `<:<` witness to improve type inference. */
  implicit def forSeqLike[R, A](implicit ev: R <:< Seq[A]): Aux[R, A] =
    new ExplodeEvidence[R] {
      type Elem = A
      def toSeq(r: R): Seq[A] = if (r == null) Seq.empty else ev(r)
    }

  implicit def forArray[A]: Aux[Array[A], A] =
    new ExplodeEvidence[Array[A]] {
      type Elem = A
      def toSeq(r: Array[A]): Seq[A] = if (r == null) Seq.empty else ArraySeq.unsafeWrapArray(r)
    }

  implicit def forOption[A]: Aux[Option[A], A] =
    new ExplodeEvidence[Option[A]] {
      type Elem = A
      def toSeq(r: Option[A]): Seq[A] = if (r == null) Seq.empty else r.toSeq
    }
}
