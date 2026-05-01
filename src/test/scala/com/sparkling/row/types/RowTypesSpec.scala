package com.sparkling.row.types

import org.scalatest.matchers.should.Matchers._
import org.scalatest.wordspec.AnyWordSpec

final class RowTypesSpec extends AnyWordSpec {

  import ValueType._

  "ValueType" should {
    "support basic construction for primitives, arrays, maps, and objects" in {
      StringType shouldBe StringType
      IntType shouldBe IntType
      LongType shouldBe LongType
      DoubleType shouldBe DoubleType
      BooleanType shouldBe BooleanType

      ArrayType(StringType) shouldBe ArrayType(StringType)
      ArrayType(IntType) shouldBe ArrayType(IntType)

      MapType(StringType, IntType) shouldBe MapType(StringType, IntType)
      MapType(LongType, ArrayType(BooleanType)) shouldBe MapType(LongType, ArrayType(BooleanType))

      val schema = RecordSchema(
        Vector(
          SchemaField("a", StringType, nullable = true),
          SchemaField("b", IntType, nullable = false)
        )
      )
      ObjectType(schema) shouldBe ObjectType(schema)
    }
  }

  "SchemaField" should {
    "be a simple runtime field descriptor" in {
      val sf = SchemaField("x", StringType, nullable = true)
      sf.name shouldBe "x"
      sf.valueType shouldBe StringType
      sf.nullable shouldBe true
    }
  }

  "RecordSchema" should {
    "support empty and single constructors" in {
      RecordSchema.empty.isEmpty shouldBe true
      RecordSchema.empty.size shouldBe 0
      RecordSchema.empty.isSingle shouldBe false
      RecordSchema.empty.columnNames shouldBe Vector.empty

      val one = RecordSchema.single("x", IntType)
      one.isEmpty shouldBe false
      one.isSingle shouldBe true
      one.size shouldBe 1
      one.columnNames shouldBe Vector("x")
      one.get("x") shouldBe Some(SchemaField("x", IntType, nullable = true))
      one.get("missing") shouldBe None
    }

    "preserve column order in columnNames and select()" in {
      val schema = RecordSchema(
        Vector(
          SchemaField("a", StringType, nullable = true),
          SchemaField("b", IntType, nullable = true),
          SchemaField("c", LongType, nullable = true),
          SchemaField("d", DoubleType, nullable = true)
        )
      )

      schema.columnNames shouldBe Vector("a", "b", "c", "d")

      val selected = schema.select(Set("d", "b"))
      selected.columnNames shouldBe Vector("b", "d")
      selected.columns.map(_.valueType) shouldBe Vector(IntType, DoubleType)
    }

    "return empty schema when select() is given an empty set" in {
      val schema = RecordSchema(
        Vector(
          SchemaField("a", StringType, nullable = true),
          SchemaField("b", IntType, nullable = true)
        )
      )

      schema.select(Set.empty) shouldBe RecordSchema.empty
    }

    "throw if select() contains unknown column names" in {
      val schema = RecordSchema(
        Vector(
          SchemaField("a", StringType, nullable = true),
          SchemaField("b", IntType, nullable = true)
        )
      )

      an[IllegalArgumentException] shouldBe thrownBy(schema.select(Set("a", "z")))
    }

    "rename only mapped columns and preserve order and types" in {
      val schema = RecordSchema(
        Vector(
          SchemaField("a", StringType, nullable = true),
          SchemaField("b", IntType, nullable = false),
          SchemaField("c", LongType, nullable = true)
        )
      )

      val renamed = schema.rename(Map("a" -> "aa", "c" -> "cc"))

      renamed.columnNames shouldBe Vector("aa", "b", "cc")
      renamed.columns shouldBe Vector(
        SchemaField("aa", StringType, nullable = true),
        SchemaField("b", IntType, nullable = false),
        SchemaField("cc", LongType, nullable = true)
      )
    }

    "leave schema unchanged when rename() is given an empty mapping" in {
      val schema = RecordSchema(
        Vector(
          SchemaField("a", StringType, nullable = true),
          SchemaField("b", IntType, nullable = false)
        )
      )

      schema.rename(Map.empty) shouldBe schema
    }

    "throw if rename() mapping contains unknown source columns" in {
      val schema = RecordSchema(
        Vector(
          SchemaField("a", StringType, nullable = true),
          SchemaField("b", IntType, nullable = true)
        )
      )

      an[IllegalArgumentException] shouldBe thrownBy(schema.rename(Map("z" -> "zz")))
    }

    "throw if rename() introduces duplicate column names" in {
      val schema = RecordSchema(
        Vector(
          SchemaField("a", StringType, nullable = true),
          SchemaField("b", IntType, nullable = true)
        )
      )

      an[IllegalArgumentException] shouldBe thrownBy(schema.rename(Map("a" -> "x", "b" -> "x")))
    }

    "make all columns nullable with makeAllNullable()" in {
      val schema = RecordSchema(
        Vector(
          SchemaField("a", StringType, nullable = false),
          SchemaField("b", IntType, nullable = true),
          SchemaField("c", LongType, nullable = false)
        )
      )

      schema.makeAllNullable.columns shouldBe Vector(
        SchemaField("a", StringType, nullable = true),
        SchemaField("b", IntType, nullable = true),
        SchemaField("c", LongType, nullable = true)
      )
    }

    "be idempotent for makeAllNullable()" in {
      val schema = RecordSchema(
        Vector(
          SchemaField("a", StringType, nullable = false),
          SchemaField("b", IntType, nullable = true)
        )
      )

      schema.makeAllNullable.makeAllNullable shouldBe schema.makeAllNullable
    }
  }
}
