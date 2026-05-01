package com.sparkling.row.convert

import org.scalatest.matchers.should.Matchers._
import org.scalatest.wordspec.AnyWordSpec

import com.sparkling.row.Row
import com.sparkling.row.codec.ValueCodec
import com.sparkling.row.types.{RecordSchema, SchemaField, ValueType}
import com.sparkling.schema.Fields

object RowConverterSpec {
  final case class NameCity(name: String, city: String)
}

final class RowConverterSpec extends AnyWordSpec {
  import RowConverterSpec._

  private def nameCityConv: RowDecoder[(String, String)] = new RowDecoder[(String, String)] {
    override val declared = Some(Fields("name", "city"))
    override def from(r: Row): (String, String) =
      (ValueCodec.string.decodeUnsafe(r.getAt(0)), ValueCodec.string.decodeUnsafe(r.getAt(1)))
  }

  // A custom ValueCodec for NameCity that encodes/decodes as an ObjectType (nested row)
  implicit private val nameCityCodec: ValueCodec[NameCity] = new ValueCodec[NameCity] {
    private val rs =
      RecordSchema(
        Vector(
          SchemaField("name", ValueType.StringType, nullable = true),
          SchemaField("city", ValueType.StringType, nullable = true)
        )
      )

    val valueType: ValueType = ValueType.ObjectType(rs)
    val nullable: Boolean = true

    def encode(t: NameCity): Any =
      if (t == null) null
      else Row.fromArray(Fields("name", "city"), Array[Any](t.name, t.city))

    def decodeUnsafe(a: Any): NameCity =
      a match {
        case r: Row =>
          NameCity(
            name = r.getAs[String]("name").orNull,
            city = r.getAs[String]("city").orNull
          )
        case null =>
          null.asInstanceOf[NameCity]
        case x =>
          throw new ClassCastException(s"Expected Row, got ${x.getClass.getName}")
      }
  }

  "RowDecoder.alignTo(actual)" should {
    "be a no-op when declared is None (Unit)" in {
      val conv = implicitly[RowDecoder[Unit]]
      val aligned = conv.alignTo(Fields("whatever"))
      aligned shouldBe conv
    }

    "be a no-op when declared is None (Row)" in {
      val conv = implicitly[RowDecoder[Row]]
      val actual = Fields("a")
      val row = Row.fromArray(actual, Array[Any]("x"))
      val aligned = conv.alignTo(actual)
      aligned shouldBe conv
      aligned.from(row) shouldBe row
    }

    "support Tuple3 decoding by ordinal" in {
      val actual = Fields("a", "b", "c")
      val row = Row.fromArray(actual, Array[Any]("x", 10L, 2.5))
      val base = implicitly[RowDecoder[(String, Long, Double)]].alignTo(Fields("x1", "x2", "x3"))
      val conv = base.alignTo(actual)
      conv.from(row) shouldBe ("x", 10L, 2.5)
    }

    "preserve the original converter behavior on repeated alignTo calls" in {
      val declared = Fields("first", "second")
      val actual1 = Fields("a", "b")
      val actual2 = Fields("x", "y")

      val base = implicitly[RowDecoder[(String, Long)]].alignTo(declared)

      val c1 = base.alignTo(actual1)
      c1.declared shouldBe Some(actual1)

      val c2 = c1.alignTo(actual2)
      c2.declared shouldBe Some(actual2)

      val row = Row.fromArray(actual2, Array[Any]("v", 1L))
      c2.from(row) shouldBe ("v", 1L)
    }

    "decode a single item against any 1-column actual schema" in {
      val declared = Fields("id")
      val actual = Fields("pkey")
      val row = Row.fromArray(actual, Array[Any]("P1"))

      val base = implicitly[RowDecoder[String]].alignTo(declared)
      val conv = base.alignTo(actual)
      conv.declared shouldBe Some(actual)
      conv.from(row) shouldBe "P1"
    }

    "decode a Tuple2 by ordinal after aligning from declared names to actual names" in {
      val declared = Fields("first", "second")
      val actual = Fields("a", "b")
      val row = Row.fromArray(actual, Array[Any]("x", 10L))

      val conv = implicitly[RowDecoder[(String, Long)]].alignTo(declared).alignTo(actual)
      conv.declared shouldBe Some(actual)
      conv.from(row) shouldBe ("x", 10L)
    }

    "decode Unit from any row" in {
      val conv = implicitly[RowDecoder[Unit]]
      val row = Row.fromArray(Fields("a"), Array[Any]("x"))
      conv.from(row) shouldBe (())
    }

    "decode via the nested-row path when actual is a single column holding a Row" in {
      val nestedSchema = Fields("name", "city")
      val nestedRow = Row.fromArray(nestedSchema, Array[Any]("alice", "nyc"))
      val actual = Fields("__packed")
      val outer = Row.fromArray(actual, Array[Any](nestedRow))

      val aligned = nameCityConv.alignTo(actual)
      aligned.from(outer) shouldBe ("alice", "nyc")
    }

    "delegate re-alignment from the nested-row wrapper back to the original converter" in {
      val actualNested = Fields("__packed")
      val actualFlat = Fields("x", "y")

      val nestedAligned = nameCityConv.alignTo(actualNested)
      nestedAligned.declared shouldBe Some(actualNested)

      val realigned = nestedAligned.alignTo(actualFlat)
      realigned.declared shouldBe Some(actualFlat)

      val row = Row.fromArray(actualFlat, Array[Any]("alice", "nyc"))
      realigned.from(row) shouldBe ("alice", "nyc")
    }

    "throw when nested field is null (nested-row path)" in {
      val actual = Fields("__packed")
      val outer = Row.fromArray(actual, Array[Any](null))
      val aligned = nameCityConv.alignTo(actual)
      an[NoSuchElementException] shouldBe thrownBy(aligned.from(outer))
    }

    "throw when nested field is not a Row (nested-row path)" in {
      val actual = Fields("__packed")
      val outer = Row.fromArray(actual, Array[Any]("not-a-row"))
      val aligned = nameCityConv.alignTo(actual)
      an[ClassCastException] shouldBe thrownBy(aligned.from(outer))
    }

    "throw if arity is incompatible and not a nested-row case" in {
      val conv = implicitly[RowDecoder[(String, Long)]]
      val ex = intercept[IllegalArgumentException] {
        conv.alignTo(Fields("one", "two", "extra"))
      }
      ex.getMessage.toLowerCase should include("cannot align")
    }

    "derive declared fields from an object-shaped ValueCodec" in {
      val conv = implicitly[RowDecoder[NameCity]]
      conv.declared shouldBe Some(Fields("name", "city"))
    }

    "decode an object-shaped item from a multi-column row" in {
      val conv = implicitly[RowDecoder[NameCity]]
      val row = Row.fromArray(Fields("name", "city"), Array[Any]("alice", "nyc"))
      conv.from(row) shouldBe NameCity("alice", "nyc")
    }

    "decode an object-shaped item after same-arity schema rebinding" in {
      val conv = implicitly[RowDecoder[NameCity]].alignTo(Fields("n", "c"))
      val row = Row.fromArray(Fields("n", "c"), Array[Any]("alice", "nyc"))
      conv.from(row) shouldBe NameCity("alice", "nyc")
    }

    "decode an object-shaped item from a nested row via alignTo(actual)" in {
      val inner = Row.fromArray(Fields("name", "city"), Array[Any]("alice", "nyc"))
      val outer = Row.fromArray(Fields("__packed"), Array[Any](inner))
      val conv = implicitly[RowDecoder[NameCity]].alignTo(Fields("__packed"))
      conv.from(outer) shouldBe NameCity("alice", "nyc")
    }

    "reorder an object-shaped item by field name in the base converter when schema order differs" in {
      val conv = implicitly[RowDecoder[NameCity]]
      val row = Row.fromArray(Fields("city", "name"), Array[Any]("nyc", "alice"))
      conv.from(row) shouldBe NameCity("alice", "nyc")
    }
  }

  "RowEncoder.alignTo(actual)" should {
    "rebind output field names without changing value order" in {
      val base = implicitly[RowEncoder[(String, Long)]]
      base.declared.names.toSeq shouldBe Seq("_1", "_2")

      val actual = Fields("pkey", "score")
      val rebound = base.alignTo(actual)

      val row = rebound.to(("P1", 7L))
      row.schema shouldBe actual
      row.getAt(0) shouldBe "P1"
      row.getAt(1) shouldBe 7L
    }

    "encode a single item as a one-column row" in {
      val conv = implicitly[RowEncoder[String]]
      conv.declared.names.toSeq shouldBe Seq("_1")

      val row = conv.to("hello")
      row.schema shouldBe conv.declared
      row.values.toSeq shouldBe Seq("hello")
      row.getAt(0) shouldBe "hello"
    }

    "encode Unit as an empty row" in {
      val conv = implicitly[RowEncoder[Unit]]
      val row = conv.to(())
      row.schema shouldBe Fields.empty
      row.values.toSeq shouldBe Seq.empty
    }

    "encode a Tuple3 as a three-column row" in {
      val conv = implicitly[RowEncoder[(String, Long, Double)]]
      conv.declared.names.toSeq shouldBe Seq("_1", "_2", "_3")

      val row = conv.to(("x", 10L, 2.5))
      row.schema shouldBe conv.declared
      row.values.toSeq shouldBe Seq("x", 10L, 2.5)
    }

    "preserve original converter behavior on repeated alignTo calls" in {
      val base = implicitly[RowEncoder[(String, Long)]]
      val c1 = base.alignTo(Fields("pkey", "score"))
      val c2 = c1.alignTo(Fields("id", "value"))

      val row = c2.to(("P1", 7L))
      row.schema shouldBe Fields("id", "value")
      row.getAt(0) shouldBe "P1"
      row.getAt(1) shouldBe 7L
    }

    "throw when multi-field output arity does not match" in {
      val base1 = implicitly[RowEncoder[(String, Long)]]
      an[IllegalArgumentException] shouldBe thrownBy(base1.alignTo(Fields("one", "two", "three")))
    }

    "derive declared fields and schema from an object-shaped ValueCodec" in {
      val conv = implicitly[RowEncoder[NameCity]]
      conv.declared shouldBe Fields("name", "city")
      conv.schema.columnNames shouldBe Vector("name", "city")
    }

    "encode an object-shaped item as a multi-column row" in {
      val conv = implicitly[RowEncoder[NameCity]]
      val row = conv.to(NameCity("alice", "nyc"))
      row.schema shouldBe Fields("name", "city")
      row.values.toSeq shouldBe Seq("alice", "nyc")
    }

    "encode a null object-shaped item as an all-null row" in {
      val conv = implicitly[RowEncoder[NameCity]]
      val row = conv.to(null.asInstanceOf[NameCity])
      row.schema shouldBe Fields("name", "city")
      row.values.toSeq shouldBe Seq(null, null)
    }

    "rebind output field names for an object-shaped item" in {
      val conv = implicitly[RowEncoder[NameCity]].alignTo(Fields("n", "c"))
      val row = conv.to(NameCity("alice", "nyc"))
      row.schema shouldBe Fields("n", "c")
      row.values.toSeq shouldBe Seq("alice", "nyc")
    }

    "wrap a tuple output as a nested row when aligning to a single output field" in {
      val conv = implicitly[RowEncoder[(String, Long)]].alignTo(Fields("payload"))
      val row = conv.to(("alice", 7L))

      row.schema shouldBe Fields("payload")
      val nested = row.get("payload").get.asInstanceOf[Row]
      nested.schema shouldBe Fields("_1", "_2")
      nested.getAt(0) shouldBe "alice"
      nested.getAt(1) shouldBe 7L
    }

    "take the fast path in to() when the base row already carries the rebound schema" in {
      val base = new RowEncoder[String] {
        override val declared: Fields = Fields("target")
        override val schema: RecordSchema =
          RecordSchema.single("target", ValueType.StringType, nullable = true)
        override def to(t: String): Row =
          Row.fromArray(Fields("target"), Array[Any](t))
      }
      val rebound = base.alignTo(Fields("target"))
      val row = rebound.to("hello")
      row.schema shouldBe Fields("target")
      row.getAt(0) shouldBe "hello"
    }

    "throw ClassCastException when an ObjectType codec returns a non-Row from encode" in {
      implicit val badCodec: ValueCodec[NameCity] = new ValueCodec[NameCity] {
        private val rs = RecordSchema(
          Vector(
            SchemaField("name", ValueType.StringType, nullable = true),
            SchemaField("city", ValueType.StringType, nullable = true)
          )
        )
        val valueType: ValueType = ValueType.ObjectType(rs)
        val nullable: Boolean = true
        def encode(t: NameCity): Any = "not-a-row"
        def decodeUnsafe(a: Any): NameCity = throw new UnsupportedOperationException
      }
      val conv = RowEncoder.fromValueCodec[NameCity](badCodec)
      an[ClassCastException] shouldBe thrownBy(conv.to(NameCity("alice", "nyc")))
    }

    "wrap an object-shaped item as a nested row when aligning to a single output field" in {
      val conv = implicitly[RowEncoder[NameCity]].alignTo(Fields("payload"))
      val row = conv.to(NameCity("alice", "nyc"))

      row.schema shouldBe Fields("payload")
      val nested = row.get("payload").get.asInstanceOf[Row]
      nested.schema shouldBe Fields("name", "city")
      nested.getAs[String]("name") shouldBe Some("alice")
      nested.getAs[String]("city") shouldBe Some("nyc")
    }

    "return the base row unchanged from same-arity alignTo when the row already has the target schema" in {
      val conv = implicitly[RowEncoder[String]].alignTo(Fields("out"))
      val row = conv.to("hello")
      row.schema shouldBe Fields("out")
      row.getAt(0) shouldBe "hello"
    }

    "support repeated alignTo on a nested-wrapped converter" in {
      val c1 = implicitly[RowEncoder[(String, Long)]].alignTo(Fields("payload"))
      c1.declared shouldBe Fields("payload")

      val c2 = c1.alignTo(Fields("wrapped"))
      c2.declared shouldBe Fields("wrapped")

      val row = c2.to(("alice", 7L))
      row.schema shouldBe Fields("wrapped")
      val nested = row.get("wrapped").get.asInstanceOf[Row]
      nested.getAt(0) shouldBe "alice"
      nested.getAt(1) shouldBe 7L
    }

    "reorder an object-shaped encoded row by field name when encode returns a different schema order" in {
      implicit val reversedCodec: ValueCodec[NameCity] = new ValueCodec[NameCity] {
        private val rs = RecordSchema(
          Vector(
            SchemaField("name", ValueType.StringType, nullable = true),
            SchemaField("city", ValueType.StringType, nullable = true)
          )
        )
        val valueType: ValueType = ValueType.ObjectType(rs)
        val nullable: Boolean = true
        def encode(t: NameCity): Any =
          if (t == null) null
          else Row.fromArray(Fields("city", "name"), Array[Any](t.city, t.name)) // reversed
        def decodeUnsafe(a: Any): NameCity = a match {
          case r: Row => NameCity(r.getAs[String]("name").orNull, r.getAs[String]("city").orNull)
          case null   => null.asInstanceOf[NameCity]
          case x      => throw new ClassCastException(s"Expected Row, got ${x.getClass.getName}")
        }
      }

      val conv = RowEncoder.fromValueCodec[NameCity](reversedCodec)
      val row = conv.to(NameCity("alice", "nyc"))

      // despite encode returning city-first, the converter normalises to declared (name, city) order
      row.schema shouldBe Fields("name", "city")
      row.getAs[String]("name") shouldBe Some("alice")
      row.getAs[String]("city") shouldBe Some("nyc")
    }
  }

  "RowDecoder (object-shaped alignTo)" should {
    "use the fast path when the row is already in the declared schema" in {
      val conv = implicitly[RowDecoder[NameCity]]
      val rebound = conv.alignTo(Fields("name", "city"))
      val row = Row.fromArray(Fields("name", "city"), Array[Any]("alice", "nyc"))
      rebound.from(row) shouldBe NameCity("alice", "nyc")
    }

    "support repeated re-alignment on the rebound object-shaped converter" in {
      val c1 = implicitly[RowDecoder[NameCity]].alignTo(Fields("n", "c"))
      c1.declared shouldBe Some(Fields("n", "c"))

      val c2 = c1.alignTo(Fields("x", "y"))
      c2.declared shouldBe Some(Fields("x", "y"))

      val row = Row.fromArray(Fields("x", "y"), Array[Any]("alice", "nyc"))
      c2.from(row) shouldBe NameCity("alice", "nyc")
    }
  }
}
