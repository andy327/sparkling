package com.sparkling.schema

import org.apache.spark.sql.types._
import org.scalatest.matchers.should.Matchers._
import org.scalatest.wordspec.AnyWordSpec

import com.sparkling.row.types._

final class SparkSchemaSpec extends AnyWordSpec {

  "SparkSchema" should {
    "map primitive ValueTypes to Spark DataTypes" in {
      SparkSchema.toSparkType(ValueType.StringType) shouldBe StringType
      SparkSchema.toSparkType(ValueType.IntType) shouldBe IntegerType
      SparkSchema.toSparkType(ValueType.LongType) shouldBe LongType
      SparkSchema.toSparkType(ValueType.DoubleType) shouldBe DoubleType
      SparkSchema.toSparkType(ValueType.BooleanType) shouldBe BooleanType
    }

    "map ArrayType recursively" in {
      val vt = ValueType.ArrayType(ValueType.IntType)
      SparkSchema.toSparkType(vt) shouldBe ArrayType(IntegerType, containsNull = true)
    }

    "map MapType recursively" in {
      val vt = ValueType.MapType(ValueType.StringType, ValueType.ArrayType(ValueType.IntType))

      SparkSchema.toSparkType(vt) shouldBe
        MapType(
          StringType,
          ArrayType(IntegerType, containsNull = true),
          valueContainsNull = true
        )
    }

    "map ObjectType to StructType recursively" in {
      val rs = RecordSchema(
        Vector(
          SchemaField("a", ValueType.StringType, nullable = true),
          SchemaField("b", ValueType.IntType, nullable = false)
        )
      )
      val dt = SparkSchema.toSparkType(ValueType.ObjectType(rs))
      dt shouldBe StructType(
        Array(
          StructField("a", StringType, nullable = true),
          StructField("b", IntegerType, nullable = false)
        )
      )
    }
  }
}
