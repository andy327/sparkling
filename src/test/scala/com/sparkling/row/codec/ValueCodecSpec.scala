package com.sparkling.row.codec

import org.scalatest.matchers.should.Matchers._
import org.scalatest.wordspec.AnyWordSpec

import com.sparkling.row.types.ValueType._

final class ValueCodecSpec extends AnyWordSpec {

  "ValueCodec" should {
    "expose correct ValueType for primitives" in {
      implicitly[ValueCodec[String]].valueType shouldBe StringType
      implicitly[ValueCodec[Int]].valueType shouldBe IntType
      implicitly[ValueCodec[Long]].valueType shouldBe LongType
      implicitly[ValueCodec[Double]].valueType shouldBe DoubleType
      implicitly[ValueCodec[Boolean]].valueType shouldBe BooleanType
    }

    "expose correct nullable for primitives" in {
      implicitly[ValueCodec[String]].nullable shouldBe true
      implicitly[ValueCodec[Int]].nullable shouldBe false
      implicitly[ValueCodec[Long]].nullable shouldBe false
      implicitly[ValueCodec[Double]].nullable shouldBe false
      implicitly[ValueCodec[Boolean]].nullable shouldBe false
    }

    "expose nullable=true for Option, collections, and maps" in {
      implicitly[ValueCodec[Option[Int]]].nullable shouldBe true
      implicitly[ValueCodec[Seq[Int]]].nullable shouldBe true
      implicitly[ValueCodec[Vector[Int]]].nullable shouldBe true
      implicitly[ValueCodec[Array[Int]]].nullable shouldBe true
      implicitly[ValueCodec[Set[Int]]].nullable shouldBe true
      implicitly[ValueCodec[Map[String, Int]]].nullable shouldBe true
    }

    "encode primitives as themselves" in {
      implicitly[ValueCodec[String]].encode("x") shouldBe "x"
      implicitly[ValueCodec[Int]].encode(7) shouldBe 7
      implicitly[ValueCodec[Long]].encode(7L) shouldBe 7L
      implicitly[ValueCodec[Double]].encode(2.5) shouldBe 2.5
      implicitly[ValueCodec[Boolean]].encode(true) shouldBe true
    }

    "decodeUnsafe primitives with basic coercions" in {
      val i = implicitly[ValueCodec[Int]]
      i.decodeUnsafe(7) shouldBe 7
      i.decodeUnsafe(7L) shouldBe 7
      i.decodeUnsafe("42") shouldBe 42

      val l = implicitly[ValueCodec[Long]]
      l.decodeUnsafe(9L) shouldBe 9L
      l.decodeUnsafe(9) shouldBe 9L
      l.decodeUnsafe("9") shouldBe 9L

      val d = implicitly[ValueCodec[Double]]
      d.decodeUnsafe(2.5) shouldBe 2.5
      d.decodeUnsafe(2.5f) shouldBe 2.5 +- 1e-6
      d.decodeUnsafe(2L) shouldBe 2.0
      d.decodeUnsafe(2) shouldBe 2.0
      d.decodeUnsafe("2.5") shouldBe 2.5
    }

    "decode primitives (with basic coercions) via safe decode" in {
      val i = implicitly[ValueCodec[Int]]
      i.decode(7) shouldBe Right(7)
      i.decode(7L) shouldBe Right(7)
      i.decode("42") shouldBe Right(42)

      val l = implicitly[ValueCodec[Long]]
      l.decode(9L) shouldBe Right(9L)
      l.decode(9) shouldBe Right(9L)
      l.decode("9") shouldBe Right(9L)

      val d = implicitly[ValueCodec[Double]]
      d.decode(2.5) shouldBe Right(2.5)
      d.decode(2.5f) shouldBe Right(2.5)
      d.decode(2L) shouldBe Right(2.0)
      d.decode(2) shouldBe Right(2.0)
      d.decode("2.5") shouldBe Right(2.5)

      val b = implicitly[ValueCodec[Boolean]]
      b.decode(true) shouldBe Right(true)
    }

    "treat null as an error for primitive codecs in both decodeUnsafe and decode" in {
      val i = implicitly[ValueCodec[Int]]
      an[NoSuchElementException] shouldBe thrownBy(i.decodeUnsafe(null))
      i.decode(null) match {
        case Left(_: NoSuchElementException) => succeed
        case other                           => fail(s"expected Left(NoSuchElementException), got $other")
      }

      val l = implicitly[ValueCodec[Long]]
      an[NoSuchElementException] shouldBe thrownBy(l.decodeUnsafe(null))
      l.decode(null) match {
        case Left(_: NoSuchElementException) => succeed
        case other                           => fail(s"expected Left(NoSuchElementException), got $other")
      }

      val d = implicitly[ValueCodec[Double]]
      an[NoSuchElementException] shouldBe thrownBy(d.decodeUnsafe(null))
      d.decode(null) match {
        case Left(_: NoSuchElementException) => succeed
        case other                           => fail(s"expected Left(NoSuchElementException), got $other")
      }

      val b = implicitly[ValueCodec[Boolean]]
      an[NoSuchElementException] shouldBe thrownBy(b.decodeUnsafe(null))
      b.decode(null) match {
        case Left(_: NoSuchElementException) => succeed
        case other                           => fail(s"expected Left(NoSuchElementException), got $other")
      }
    }

    "allow null for String (Spark-like semantics)" in {
      val s = implicitly[ValueCodec[String]]
      s.decodeUnsafe(null) shouldBe (null: Any)
      s.decode(null) shouldBe Right(null)
    }

    "return Left(NumberFormatException) when String parsing fails for numeric codecs" in {
      val i = implicitly[ValueCodec[Int]]
      i.decode("notAnInt") match {
        case Left(_: NumberFormatException) => succeed
        case other                          => fail(s"expected Left(NumberFormatException), got $other")
      }

      val d = implicitly[ValueCodec[Double]]
      d.decode("notADouble") match {
        case Left(_: NumberFormatException) => succeed
        case other                          => fail(s"expected Left(NumberFormatException), got $other")
      }

      val l = implicitly[ValueCodec[Long]]
      l.decode("notALong") match {
        case Left(_: NumberFormatException) => succeed
        case other                          => fail(s"expected Left(NumberFormatException), got $other")
      }
    }

    "return Left(ClassCastException) on incompatible types" in {
      val i = implicitly[ValueCodec[Int]]
      i.decode(true) match {
        case Left(_: ClassCastException) => succeed
        case other                       => fail(s"expected Left(ClassCastException), got $other")
      }

      val l = implicitly[ValueCodec[Long]]
      l.decode(true) match {
        case Left(_: ClassCastException) => succeed
        case other                       => fail(s"expected Left(ClassCastException), got $other")
      }

      val d = implicitly[ValueCodec[Double]]
      d.decode(true) match {
        case Left(_: ClassCastException) => succeed
        case other                       => fail(s"expected Left(ClassCastException), got $other")
      }

      val b = implicitly[ValueCodec[Boolean]]
      b.decode(1) match {
        case Left(_: ClassCastException) => succeed
        case other                       => fail(s"expected Left(ClassCastException), got $other")
      }

      val s = implicitly[ValueCodec[String]]
      s.decode(123) match {
        case Left(_: ClassCastException) => succeed
        case other                       => fail(s"expected Left(ClassCastException), got $other")
      }
    }

    "throw ClassCastException from decodeUnsafe on incompatible types" in {
      an[ClassCastException] shouldBe thrownBy(implicitly[ValueCodec[Int]].decodeUnsafe(true))
      an[ClassCastException] shouldBe thrownBy(implicitly[ValueCodec[Long]].decodeUnsafe(true))
      an[ClassCastException] shouldBe thrownBy(implicitly[ValueCodec[Double]].decodeUnsafe(true))
    }

    "Boolean does not coerce from String" in {
      val b = implicitly[ValueCodec[Boolean]]
      an[ClassCastException] shouldBe thrownBy(b.decodeUnsafe("true"))
      b.decode("true") match {
        case Left(_: ClassCastException) => succeed
        case other                       => fail(s"expected Left(ClassCastException), got $other")
      }
    }

    "support Option codec: null <-> None, otherwise decode/encode Some(value)" in {
      val c = implicitly[ValueCodec[Option[Int]]]
      c.valueType shouldBe IntType

      c.encode(None) shouldBe (null: Any)
      c.encode(Some(5)) shouldBe 5
      c.encode((null: Any).asInstanceOf[Option[Int]]) shouldBe (null: Any)

      c.decodeUnsafe(null) shouldBe None
      c.decodeUnsafe(3) shouldBe Some(3)
      c.decodeUnsafe("7") shouldBe Some(7)

      c.decode(null) shouldBe Right(None)
      c.decode(3) shouldBe Right(Some(3))

      c.decode("bad") match {
        case Left(_: NumberFormatException) => succeed
        case other                          => fail(s"expected Left(NumberFormatException), got $other")
      }
    }

    "support Seq codec: accepts Iterable and Array, encodes as Vector" in {
      val c = implicitly[ValueCodec[Seq[Int]]]
      c.valueType shouldBe ArrayType(IntType)
      c.nullable shouldBe true

      c.encode(Seq(1, 2, 3)) shouldBe Vector(1, 2, 3)

      c.decodeUnsafe(Seq(1, 2, 3)) shouldBe Seq(1, 2, 3)
      c.decodeUnsafe(List(4, 5)) shouldBe Seq(4, 5)
      c.decodeUnsafe(Array[Any](6, 7)) shouldBe Seq(6, 7)

      an[NoSuchElementException] shouldBe thrownBy(c.decodeUnsafe(null))
      an[ClassCastException] shouldBe thrownBy(c.decodeUnsafe("not-a-seq"))
    }

    "support Array codec: accepts Iterable and Array, encodes as Vector" in {
      val c = implicitly[ValueCodec[Array[Int]]]
      c.valueType shouldBe ArrayType(IntType)
      c.nullable shouldBe true

      c.decodeUnsafe(Seq(1, 2, 3)).toSeq shouldBe Seq(1, 2, 3)
      c.decodeUnsafe(Array[Any](4, 5)).toSeq shouldBe Seq(4, 5)
      c.encode(Array(7, 8)) shouldBe Vector(7, 8)

      an[NoSuchElementException] shouldBe thrownBy(c.decodeUnsafe(null))
      an[ClassCastException] shouldBe thrownBy(c.decodeUnsafe("not-an-array"))
    }

    "support Vector codec" in {
      val c = implicitly[ValueCodec[Vector[Int]]]
      c.valueType shouldBe ArrayType(IntType)

      c.encode(Vector(1, 2, 3)) shouldBe Vector(1, 2, 3)

      c.decodeUnsafe(Vector(1, 2, 3)) shouldBe Vector(1, 2, 3)
      c.decodeUnsafe(List(1, 2, 3)) shouldBe Vector(1, 2, 3)

      an[NoSuchElementException] shouldBe thrownBy(c.decodeUnsafe(null))
      an[ClassCastException] shouldBe thrownBy(c.decodeUnsafe("notASeq"))
    }

    "support Set codec: accepts Iterable (including Set) and Array, encodes as Vector" in {
      val c = implicitly[ValueCodec[Set[Int]]]
      c.valueType shouldBe ArrayType(IntType)

      // encode stores as Vector for a stable representation
      c.encode(Set(1, 2, 3)) match {
        case v: Vector[_] => v.toSet shouldBe Set(1, 2, 3)
        case other        => fail(s"expected Vector, got ${other.getClass.getName}: $other")
      }

      // decode accepts any Iterable, including Set stored directly in a row cell
      c.decodeUnsafe(Set(1, 2, 3)) shouldBe Set(1, 2, 3)
      c.decodeUnsafe(Vector(1, 2, 3)) shouldBe Set(1, 2, 3)
      c.decodeUnsafe(List(1, 2, 3)) shouldBe Set(1, 2, 3)
      c.decodeUnsafe(Array[Any](4, 4, 5)) shouldBe Set(4, 5)

      an[NoSuchElementException] shouldBe thrownBy(c.decodeUnsafe(null))
      an[ClassCastException] shouldBe thrownBy(c.decodeUnsafe("notAnIterable"))
    }

    "support Map codec: accepts Scala Map and java.util.Map" in {
      val c = implicitly[ValueCodec[Map[String, Int]]]
      c.valueType shouldBe MapType(StringType, IntType)
      c.nullable shouldBe true

      c.decodeUnsafe(Map("a" -> 1, "b" -> 2)) shouldBe Map("a" -> 1, "b" -> 2)

      val jm = new java.util.HashMap[String, Int]()
      jm.put("x", 7); jm.put("y", 9)
      c.decodeUnsafe(jm) shouldBe Map("x" -> 7, "y" -> 9)

      c.encode(Map("p" -> 3)) shouldBe Map("p" -> 3)

      an[NoSuchElementException] shouldBe thrownBy(c.decodeUnsafe(null))
      an[ClassCastException] shouldBe thrownBy(c.decodeUnsafe("not-a-map"))
    }

    "encode null collection/map as null" in {
      implicitly[ValueCodec[Vector[Int]]].encode(null) shouldBe (null: Any)
      implicitly[ValueCodec[Set[Int]]].encode(null) shouldBe (null: Any)
      implicitly[ValueCodec[Seq[Int]]].encode(null) shouldBe (null: Any)
      implicitly[ValueCodec[Array[Int]]].encode(null) shouldBe (null: Any)
      implicitly[ValueCodec[Map[String, Int]]].encode(null) shouldBe (null: Any)
    }

    "support Vector codec: Array input branch in decodeUnsafe" in {
      val c = implicitly[ValueCodec[Vector[Int]]]
      c.decodeUnsafe(Array[Any](1, 2, 3)) shouldBe Vector(1, 2, 3)
    }

    "support forRow: valueType, nullable, encode, and decodeUnsafe" in {
      import com.sparkling.row.Row
      import com.sparkling.row.types.{RecordSchema, SchemaField}
      import com.sparkling.row.types.ValueType.ObjectType
      import com.sparkling.schema.Fields

      val schema = RecordSchema(
        Vector(
          SchemaField("x", IntType, nullable = false),
          SchemaField("y", StringType, nullable = true)
        )
      )
      val c = ValueCodec.forRow(schema)

      c.valueType shouldBe ObjectType(schema)
      c.nullable shouldBe true

      val row = Row.fromArray(Fields("x", "y"), Array[Any](1, "hello"))
      c.encode(row) shouldBe row
      c.decodeUnsafe(row) shouldBe row

      an[NoSuchElementException] shouldBe thrownBy(c.decodeUnsafe(null))
      an[ClassCastException] shouldBe thrownBy(c.decodeUnsafe("not-a-row"))
    }
  }
}
