package com.sparkling.syntax

import scala.language.implicitConversions

import com.sparkling.schema.{Field, Fields}

/** Syntax helpers so call sites can use Strings and tuples while Frame methods accept `Field` and `Fields`.
  *
  * Supported shapes:
  *   - Field refs: `"a"`
  *   - Fields refs: `"a"`, `("a","b")`, `("a","b","c")`, ... up to 22, `Seq[String]`
  *   - Mapping refs to Fields: `"a" -> "b"`, `"a" -> ("b1","b2")`, `("a","b") -> ("x","y")`, ... up to 22
  *   - Mapping refs to a single Field: `("a","b") -> "out"` (used by ops like `coalesce((Fields, Field))`)
  */
trait FieldsSyntax extends BoilerplateFieldsSyntax with BoilerplateToFieldsTupleInstances {

  implicit def stringToField(name: String): Field =
    Field(name)

  implicit def stringToFields(name: String): Fields =
    Fields(name)

  implicit def seqStringToFields(names: Seq[String]): Fields =
    Fields.fromSeq(names)

  trait ToFields[A] extends Serializable {
    def toFields(a: A): Fields
  }

  object ToFields {
    implicit val fromString: ToFields[String] =
      (a: String) => Fields(a)

    implicit val fromField: ToFields[Field] =
      (f: Field) => Fields.one(f)

    implicit val fromFields: ToFields[Fields] =
      (fs: Fields) => fs
  }

  trait ToField[A] extends Serializable {
    def toField(a: A): Field
  }

  object ToField {
    implicit val fromString: ToField[String] =
      (s: String) => Field(s)

    implicit val fromField: ToField[Field] =
      (f: Field) => f

    implicit val fromFields: ToField[Fields] =
      (fs: Fields) => {
        require(fs.isSingle, s"expected exactly one field but got ${fs.size}: ${fs.names.mkString(", ")}")
        Field(fs.names.head)
      }
  }

  implicit def fieldsToFieldsMapping[A, B](m: (A, B))(implicit
      a: ToFields[A],
      b: ToFields[B]
  ): (Fields, Fields) =
    (a.toFields(m._1), b.toFields(m._2))

  implicit def fieldsToFieldMapping[A, B](m: (A, B))(implicit
      a: ToFields[A],
      b: ToField[B]
  ): (Fields, Field) =
    (a.toFields(m._1), b.toField(m._2))

  implicit def fieldToFieldsMapping[A, B](m: (A, B))(implicit
      a: ToField[A],
      b: ToFields[B]
  ): (Field, Fields) =
    (a.toField(m._1), b.toFields(m._2))
}

object FieldsSyntax extends FieldsSyntax
