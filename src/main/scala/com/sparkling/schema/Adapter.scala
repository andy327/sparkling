package com.sparkling.schema

/** Type class for converting arbitrary values into a single [[Field]].
  *
  * Keeps APIs flexible while preserving the strong Field abstraction.
  */
trait ToField[-A] {
  def toField(a: A): Field
}

object ToField {

  /** Summoner syntax: `ToField[A].toField(a)` */
  def apply[A](implicit ev: ToField[A]): ToField[A] = ev

  /** Pass-through for an existing Field. */
  implicit val fromField: ToField[Field] =
    (f: Field) => f

  /** Safe construction from a raw String. */
  implicit val fromString: ToField[String] =
    (s: String) => Field(s)
}

/** Type class for converting arbitrary values into a [[Fields]] collection.
  *
  * Generalizes "one or more names" without leaking global implicit conversions. Supports a single [[Field]]/`String`,
  * an existing [[Fields]], or any `IterableOnce[String]`.
  */
trait ToFields[-A] {
  def toFields(a: A): Fields
}

object ToFields {

  /** Summoner syntax: `ToFields[A].toFields(a)` */
  def apply[A](implicit ev: ToFields[A]): ToFields[A] = ev

  /** Pass-through for an existing Fields. */
  implicit val fromFields: ToFields[Fields] =
    (fs: Fields) => fs

  /** Wrap a single Field as a one-element Fields. */
  implicit val fromField: ToFields[Field] =
    (f: Field) => Fields.one(f)

  /** Wrap a single String as a one-element Fields. */
  implicit val fromString: ToFields[String] =
    (s: String) => Fields(s)

  /** Convert any collection of Strings into a validated Fields. */
  implicit def fromIterable[S <: IterableOnce[String]]: ToFields[S] =
    (s: S) => Fields.fromSeq(s.iterator.toSeq)
}
