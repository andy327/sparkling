package com.sparkling.row.codec

import org.scalatest.matchers.should.Matchers._
import org.scalatest.wordspec.AnyWordSpec
import shapeless._

import com.sparkling.row.Row
import com.sparkling.row.types.ValueType._
import com.sparkling.schema.Fields

final private case class Point(x: Double, y: Double)
final private case class Person(name: String, age: Int)
final private case class One(i: Int)

final class RecordCodecSpec extends AnyWordSpec {

  import RecordCodec._

  "RecordCodec" should {
    "derive schema for a case class using field names and ValueCodec types" in {
      val rc = implicitly[RecordCodec[Point]]
      rc.schema.columnNames shouldBe Vector("x", "y")
      rc.schema.columns.map(_.valueType) shouldBe Vector(DoubleType, DoubleType)

      val rc2 = implicitly[RecordCodec[Person]]
      rc2.schema.columnNames shouldBe Vector("name", "age")
      rc2.schema.columns.map(_.valueType) shouldBe Vector(StringType, IntType)
    }

    "encode and decode a case class round-trip" in {
      val rc = implicitly[RecordCodec[Point]]
      val row = rc.encodeRecord(Point(1.0, 2.0))

      row.get("x") shouldBe Some(1.0)
      row.get("y") shouldBe Some(2.0)

      rc.decodeRecord(row) shouldBe Right(Point(1.0, 2.0))
    }

    "return Left on decode if a field is missing/null and the ValueCodec requires a value" in {
      val rc = implicitly[RecordCodec[Person]]
      val row = Row.fromArray(Fields("name", "age"), Array("andy", null))
      rc.decodeRecord(row) match {
        case Left(_: NoSuchElementException) => succeed
        case other                           => fail(s"expected Left(NoSuchElementException), got $other")
      }
    }

    "rebind by renaming when outputs arity matches" in {
      val rc0 = implicitly[RecordCodec[Point]]
      val rc = rc0.withFields(Fields("a", "b"))

      rc.schema.columnNames shouldBe Vector("a", "b")

      val row = rc.encodeRecord(Point(3.0, 4.0))
      row.get("a") shouldBe Some(3.0)
      row.get("b") shouldBe Some(4.0)

      // decode should succeed on the rebound codec using the rebound column names
      rc.decodeRecord(row) shouldBe Right(Point(3.0, 4.0))
    }

    "rebind by nesting when outputs has a single name and codec schema has multiple columns" in {
      val rc0 = implicitly[RecordCodec[Point]]
      val rc = rc0.withFields(Fields("pt"))

      rc.schema.columnNames shouldBe Vector("pt")

      val row = rc.encodeRecord(Point(1.5, 2.5))
      val nested = row.get("pt").get.asInstanceOf[Row]
      nested.get("x") shouldBe Some(1.5)
      nested.get("y") shouldBe Some(2.5)

      rc.decodeRecord(row) shouldBe Right(Point(1.5, 2.5))
    }

    "support withFields on a nested codec (nested.withFields)" in {
      val base = implicitly[RecordCodec[Point]]
      val nested0 = base.withFields(Fields("pt"))

      nested0.schema.columnNames shouldBe Vector("pt")

      // rebinding nested codec from pt -> p (single column rename)
      val nested1 = nested0.withFields(Fields("p"))
      nested1.schema.columnNames shouldBe Vector("p")

      val row = nested1.encodeRecord(Point(1.0, 2.0))
      row.get("p") match {
        case Some(r: Row) =>
          r.get("x") shouldBe Some(1.0)
          r.get("y") shouldBe Some(2.0)
        case other =>
          fail(s"expected nested Row in column 'p', got $other")
      }

      nested1.decodeRecord(row) shouldBe Right(Point(1.0, 2.0))
    }

    "throw on incompatible arity rebinding" in {
      val rc0 = implicitly[RecordCodec[Point]]
      an[IllegalArgumentException] shouldBe thrownBy(rc0.withFields(Fields("a", "b", "c")))
    }

    "support scalar RecordCodec and rebinding to a real output name" in {
      val rc0 = implicitly[RecordCodec[Int]]
      rc0.schema.columnNames shouldBe Vector("_1")

      val rc = rc0.withFields(Fields("i"))
      rc.schema.columnNames shouldBe Vector("i")

      val row = rc.encodeRecord(7)
      row.get("i") shouldBe Some(7)
      rc.decodeRecord(row) shouldBe Right(7)
    }

    "support rebinding scalar codec to a single output name via rename (not nesting)" in {
      val rc0 = implicitly[RecordCodec[Int]]
      val rc = rc0.withFields(Fields("nested"))

      rc.schema.columnNames shouldBe Vector("nested")

      val row = rc.encodeRecord(5)
      row.get("nested") shouldBe Some(5)
      rc.decodeRecord(row) shouldBe Right(5)
    }

    "support chaining withFields on a renamed codec (renamed.withFields)" in {
      val base = implicitly[RecordCodec[Point]]

      val rc1 = base.withFields(Fields("a", "b"))
      rc1.schema.columnNames shouldBe Vector("a", "b")

      val rc2 = rc1.withFields(Fields("c", "d"))
      rc2.schema.columnNames shouldBe Vector("c", "d")

      val row = rc2.encodeRecord(Point(10.0, 20.0))
      row.get("c") shouldBe Some(10.0)
      row.get("d") shouldBe Some(20.0)

      rc2.decodeRecord(row) shouldBe Right(Point(10.0, 20.0))
    }

    "support hnil.withFields (empty schema can be rebound to empty fields)" in {
      val rc0 = implicitly[RecordCodec[HNil]]
      rc0.schema.columnNames shouldBe Vector.empty

      val rc1 = rc0.withFields(Fields.empty)
      rc1.schema.columnNames shouldBe Vector.empty

      val row = rc1.encodeRecord(HNil)
      // should still be an empty row under an empty Fields
      rc1.decodeRecord(row) shouldBe Right(HNil)
    }

    "support hcons.withFields via a single-field product and allow rebinding" in {
      val base = implicitly[RecordCodec[One]]
      base.schema.columnNames shouldBe Vector("i")

      val rc = base.withFields(Fields("x"))
      rc.schema.columnNames shouldBe Vector("x")

      val row = rc.encodeRecord(One(7))
      row.get("x") shouldBe Some(7)

      rc.decodeRecord(row) shouldBe Right(One(7))
    }

    "support withFields on the underlying labelled HList codec (exercise hcons.withFields)" in {
      val gen = LabelledGeneric[One]
      type Repr = gen.Repr // FieldType[Witness.`'i`.T, Int] :: HNil

      val rc0 = implicitly[RecordCodec[Repr]]
      rc0.schema.columnNames shouldBe Vector("i")

      val rc = rc0.withFields(Fields("x"))
      rc.schema.columnNames shouldBe Vector("x")

      val row = rc.encodeRecord(gen.to(One(7)))
      row.get("x") shouldBe Some(7)

      rc.decodeRecord(row) shouldBe Right(gen.to(One(7)))
    }

    "support chaining withFields on a single-field product (exercise renamed wrapper on hcons/product)" in {
      val base = implicitly[RecordCodec[One]]
      val rc1 = base.withFields(Fields("x"))
      val rc2 = rc1.withFields(Fields("y"))

      val row = rc2.encodeRecord(One(9))
      row.get("y") shouldBe Some(9)
      rc2.decodeRecord(row) shouldBe Right(One(9))
    }

    "return values array directly from encodeValues without wrapping in a Row" in {
      val rc = implicitly[RecordCodec[Point]]
      val values = rc.encodeValues(Point(1.0, 2.0))
      values shouldBe Array(1.0, 2.0)
    }

    "return values aligned to rebound column order from encodeValues on a renamed codec" in {
      val rc = implicitly[RecordCodec[Point]].withFields(Fields("a", "b"))
      val values = rc.encodeValues(Point(3.0, 4.0))
      values shouldBe Array(3.0, 4.0)
    }

    "return Left(NoSuchElementException) when decoding a nested codec and the outer field is missing/null" in {
      val base = implicitly[RecordCodec[Point]]
      val rc = base.withFields(Fields("pt"))

      val missing = Row.fromArray(Fields("other"), Array("x"))
      rc.decodeRecord(missing) match {
        case Left(_: NoSuchElementException) => succeed
        case other                           => fail(s"expected Left(NoSuchElementException), got $other")
      }

      val isNull = Row.fromArray(Fields("pt"), Array(null))
      rc.decodeRecord(isNull) match {
        case Left(_: NoSuchElementException) => succeed
        case other                           => fail(s"expected Left(NoSuchElementException), got $other")
      }
    }

    "return Left(ClassCastException) when decoding a nested codec and the outer field is not a Row" in {
      val base = implicitly[RecordCodec[Point]]
      val rc = base.withFields(Fields("pt"))

      val bad = Row.fromArray(Fields("pt"), Array("notARow"))
      rc.decodeRecord(bad) match {
        case Left(_: ClassCastException) => succeed
        case other                       => fail(s"expected Left(ClassCastException), got $other")
      }
    }
  }
}
