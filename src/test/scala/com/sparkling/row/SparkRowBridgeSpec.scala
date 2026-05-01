package com.sparkling.row

import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.scalatest.matchers.should.Matchers._
import org.scalatest.wordspec.AnyWordSpec

import com.sparkling.row.types.{RecordSchema, SchemaField, ValueType}
import com.sparkling.schema.Fields

final class SparkRowBridgeSpec extends AnyWordSpec {

  "SparkRowBridge.fromSparkRow" should {
    "convert a Spark struct row to a sparkling row and preserve nulls" in {
      val schema = StructType(
        Seq(
          StructField("a", IntegerType, nullable = true),
          StructField("b", StringType, nullable = true)
        )
      )

      val out = SparkRowBridge.fromSparkRow(Row(1, null), schema)

      out.schema shouldBe Fields("a", "b")
      out.getAs[Int]("a") shouldBe Some(1)
      out.isNullAt(1) shouldBe true
    }

    "return null when converting a null Spark struct row" in {
      val schema =
        StructType(Seq(StructField("a", IntegerType, nullable = true)))

      SparkRowBridge.fromSparkRow(null, schema) shouldBe null
    }

    "convert a nested Spark struct value into a nested sparkling row" in {
      val schema = StructType(
        Seq(
          StructField("id", IntegerType, nullable = false),
          StructField(
            "nested",
            StructType(
              Seq(
                StructField("x", IntegerType, nullable = false),
                StructField("y", StringType, nullable = true)
              )
            ),
            nullable = true
          )
        )
      )

      val out = SparkRowBridge.fromSparkRow(
        Row(1, Row(7, "hi")),
        schema
      )

      out.schema shouldBe Fields("id", "nested")
      out.getAs[Int]("id") shouldBe Some(1)

      val nested = out.get("nested").get.asInstanceOf[com.sparkling.row.Row]
      nested.schema shouldBe Fields("x", "y")
      nested.getAs[Int]("x") shouldBe Some(7)
      nested.getAs[String]("y") shouldBe Some("hi")
    }

    "preserve null nested struct values" in {
      val schema = StructType(
        Seq(
          StructField("id", IntegerType, nullable = false),
          StructField(
            "nested",
            StructType(
              Seq(
                StructField("x", IntegerType, nullable = false)
              )
            ),
            nullable = true
          )
        )
      )

      val out = SparkRowBridge.fromSparkRow(
        Row(1, null),
        schema
      )

      out.getAs[Int]("id") shouldBe Some(1)
      out.get("nested") shouldBe None
      out.isNullAt(1) shouldBe true
    }
  }

  "SparkRowBridge.toSparkRow" should {
    "convert a sparkling row into a Spark row in schema order" in {
      val schema =
        RecordSchema(
          Vector(
            SchemaField("a", ValueType.IntType, nullable = false),
            SchemaField("b", ValueType.StringType, nullable = true)
          )
        )

      val base =
        com.sparkling.row.Row.fromArray(
          Fields("a", "b"),
          Array[Any](1, "x")
        )

      SparkRowBridge.toSparkRow(base, schema) shouldBe Row(1, "x")
    }

    "preserve null field values" in {
      val schema =
        RecordSchema(
          Vector(
            SchemaField("a", ValueType.IntType, nullable = true),
            SchemaField("b", ValueType.StringType, nullable = true)
          )
        )

      val base =
        com.sparkling.row.Row.fromArray(
          Fields("a", "b"),
          Array[Any](null, "x")
        )

      SparkRowBridge.toSparkRow(base, schema) shouldBe Row(null, "x")
    }

    "return a Spark row of nulls when the sparkling row itself is null" in {
      val schema =
        RecordSchema(
          Vector(
            SchemaField("a", ValueType.IntType, nullable = true),
            SchemaField("b", ValueType.StringType, nullable = true)
          )
        )

      SparkRowBridge.toSparkRow(null, schema) shouldBe Row(null, null)
    }

    "convert a nested sparkling row into a nested Spark row for ObjectType" in {
      val nestedSchema =
        RecordSchema(
          Vector(
            SchemaField("x", ValueType.IntType, nullable = false),
            SchemaField("y", ValueType.StringType, nullable = true)
          )
        )

      val schema =
        RecordSchema(
          Vector(
            SchemaField("id", ValueType.IntType, nullable = false),
            SchemaField("nested", ValueType.ObjectType(nestedSchema), nullable = true)
          )
        )

      val nested =
        com.sparkling.row.Row.fromArray(
          Fields("x", "y"),
          Array[Any](7, "hi")
        )

      val base =
        com.sparkling.row.Row.fromArray(
          Fields("id", "nested"),
          Array[Any](1, nested)
        )

      SparkRowBridge.toSparkRow(base, schema) shouldBe Row(1, Row(7, "hi"))
    }

    "preserve null nested object values" in {
      val nestedSchema =
        RecordSchema(
          Vector(
            SchemaField("x", ValueType.IntType, nullable = false)
          )
        )

      val schema =
        RecordSchema(
          Vector(
            SchemaField("id", ValueType.IntType, nullable = false),
            SchemaField("nested", ValueType.ObjectType(nestedSchema), nullable = true)
          )
        )

      val base =
        com.sparkling.row.Row.fromArray(
          Fields("id", "nested"),
          Array[Any](1, null)
        )

      SparkRowBridge.toSparkRow(base, schema) shouldBe Row(1, null)
    }
  }
}
