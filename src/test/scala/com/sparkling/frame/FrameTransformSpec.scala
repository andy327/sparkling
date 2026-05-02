package com.sparkling.frame

import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.{functions => sqlf, Row}
import org.scalatest.matchers.should.Matchers._
import org.scalatest.wordspec.AnyWordSpec

import com.sparkling.schema.{Field, Fields}
import com.sparkling.syntax.FieldsSyntax._
import com.sparkling.syntax.FrameSyntax._
import com.sparkling.testkit.SparkSuite

final class FrameTransformSpec extends AnyWordSpec with SparkSuite {
  import spark.implicits._

  "Frame.flatten" should {
    "explode an array column into one row per element" in {
      val df = Seq(Seq(1, 2, 3)).toDF("arr")
      val out = df.frame.flatten("arr" -> "elem").df

      out.columns.toSeq shouldBe Seq("elem")
      out.orderBy("elem").as[Int].collect().toList shouldBe List(1, 2, 3)
    }

    "explode an array-of-structs into multiple output columns" in {
      val df = Seq(Seq((1, "a"), (2, "b"))).toDF("arr")
      val out = df.frame.flatten("arr" -> ("x", "y")).df

      out.columns.toSeq shouldBe Seq("x", "y")
      out.orderBy("x").collect().toList shouldBe List(Row(1, "a"), Row(2, "b"))
    }

    "produce one null row for a null array when outer = true" in {
      val df = spark.createDataFrame(
        spark.sparkContext.parallelize(Seq(Row(null))),
        org.apache.spark.sql.types.StructType(
          Seq(
            org.apache.spark.sql.types.StructField(
              "arr",
              org.apache.spark.sql.types.ArrayType(org.apache.spark.sql.types.IntegerType)
            )
          )
        )
      )
      val out = df.frame.flatten("arr" -> "elem", outer = true).df

      out.collect().toList shouldBe List(Row(null))
    }

    "drop rows with null arrays when outer = false (default)" in {
      val df = spark.createDataFrame(
        spark.sparkContext.parallelize(Seq(Row(null))),
        org.apache.spark.sql.types.StructType(
          Seq(
            org.apache.spark.sql.types.StructField(
              "arr",
              org.apache.spark.sql.types.ArrayType(org.apache.spark.sql.types.IntegerType)
            )
          )
        )
      )
      val out = df.frame.flatten("arr" -> "elem").df

      out.collect().toList shouldBe Nil
    }

    "throw when output fields are empty" in {
      val df = Seq(Seq(1)).toDF("arr")
      val ex = intercept[IllegalArgumentException] {
        df.frame.flatten("arr" -> Fields.empty)
      }
      ex.getMessage should include("flatten requires at least one output field")
    }
  }

  "Frame.unpivot" should {
    "rotate input columns into key/value rows" in {
      val df = Seq((1, 10, 20)).toDF("id", "a", "b")
      val out = df.frame
        .unpivot(("a", "b"), Field("col"), Field("val"))
        .orderBy("col")
        .df

      out.columns.toSeq shouldBe Seq("id", "col", "val")
      out.collect().toList shouldBe List(Row(1, "a", 10), Row(1, "b", 20))
    }

    "preserve non-pivoted columns" in {
      val df = Seq((1, "x", 10, 20)).toDF("id", "tag", "a", "b")
      val out = df.frame
        .unpivot(("a", "b"), Field("col"), Field("val"))
        .orderBy("col")
        .df

      out.columns.toSeq shouldBe Seq("id", "tag", "col", "val")
      out.select("id", "tag").distinct().collect().toList shouldBe List(Row(1, "x"))
    }

    "throw when input fields are empty" in {
      val df = Seq((1, "a")).toDF("x", "y")
      val ex = intercept[IllegalArgumentException] {
        df.frame.unpivot(Fields.empty, Field("k"), Field("v"))
      }
      ex.getMessage should include("unpivot requires at least one input field")
    }

    "throw when an input field does not exist" in {
      val df = Seq((1, "a")).toDF("x", "y")
      val ex = intercept[IllegalArgumentException] {
        df.frame.unpivot("missing", Field("k"), Field("v"))
      }
      ex.getMessage should include("unpivot input fields must exist")
    }
  }

  "Frame.deepUnpivot" should {
    "flatten scalar columns into (path, value) rows" in {
      val df = Seq((1, "hello")).toDF("id", "msg")
      val out = df.frame
        .deepUnpivot("msg", Field("path"), Field("value"))
        .orderBy("path")
        .df

      out.columns.toSeq shouldBe Seq("id", "path", "value")
      out.collect().toList shouldBe List(Row(1, "msg", "hello"))
    }

    "emit a dot-separated path for nested struct fields" in {
      val df = Seq((1, "a")).toDF("id", "name")
        .select(sqlf.col("id"), sqlf.struct(sqlf.col("name")).as("s"))

      val out = df.frame
        .deepUnpivot("s", Field("path"), Field("value"))
        .df

      out.columns.toSeq shouldBe Seq("id", "path", "value")
      out.collect().toList shouldBe List(Row(1, "s.name", "a"))
    }

    "emit one null row for a null input column" in {
      val df = spark.createDataFrame(
        spark.sparkContext.parallelize(Seq(Row(1, null: String))),
        org.apache.spark.sql.types.StructType(
          Seq(
            org.apache.spark.sql.types.StructField("id", org.apache.spark.sql.types.IntegerType),
            org.apache.spark.sql.types.StructField("v", org.apache.spark.sql.types.StringType)
          )
        )
      )
      val out = df.frame
        .deepUnpivot("v", Field("path"), Field("value"))
        .df

      out.collect().toList shouldBe List(Row(1, "v", null))
    }

    "emit one row per array element sharing the parent path" in {
      val df = Seq((1, Seq(10, 20))).toDF("id", "arr")
      val out = df.frame
        .deepUnpivot("arr", Field("path"), Field("value"))
        .orderBy("path")
        .df

      out.columns.toSeq shouldBe Seq("id", "path", "value")
      out.collect().toList shouldBe List(Row(1, "arr", "10"), Row(1, "arr", "20"))
    }

    "emit one row per map entry with a dot-separated key path" in {
      val df = Seq((1, Map("x" -> 1, "y" -> 2))).toDF("id", "m")
      val out = df.frame
        .deepUnpivot("m", Field("path"), Field("value"))
        .orderBy("path")
        .df

      out.columns.toSeq shouldBe Seq("id", "path", "value")
      out.collect().toList shouldBe List(Row(1, "m.x", "1"), Row(1, "m.y", "2"))
    }

    "throw when input fields are empty" in {
      val df = Seq((1, "a")).toDF("x", "y")
      val ex = intercept[IllegalArgumentException] {
        df.frame.deepUnpivot(Fields.empty, Field("k"), Field("v"))
      }
      ex.getMessage should include("deepUnpivot requires at least one input field")
    }

    "throw when an input field does not exist" in {
      val df = Seq((1, "a")).toDF("x", "y")
      val ex = intercept[IllegalArgumentException] {
        df.frame.deepUnpivot("missing", Field("k"), Field("v"))
      }
      ex.getMessage should include("deepUnpivot input fields must exist")
    }
  }

  "Frame.uniqueId" should {
    "add a non-negative Long ID column" in {
      val df = Seq(1, 2, 3).toDF("x")
      val out = df.frame.uniqueId(Field("id")).df

      out.columns.toSeq should contain("id")
      out.select("id").as[Long].collect().foreach(_ should be >= 0L)
    }

    "produce distinct IDs within a single job" in {
      val df = Seq(1, 2, 3).toDF("x")
      val ids = df.frame.uniqueId(Field("id")).df.select("id").as[Long].collect().toSeq
      ids.distinct.size shouldBe ids.size
    }
  }

  "Frame.uuid" should {
    "add a UUID string column matching the standard format" in {
      val uuidPattern = """[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}""".r
      val df = Seq(1, 2).toDF("x")
      val out = df.frame.uuid(Field("id")).df

      out.columns.toSeq should contain("id")
      out.select("id").as[String].collect().foreach { id =>
        uuidPattern.matches(id) shouldBe true
      }
    }

    "produce distinct UUIDs per row" in {
      val df = Seq(1, 2, 3).toDF("x")
      val ids = df.frame.uuid(Field("id")).df.select("id").as[String].collect().toSeq
      ids.distinct.size shouldBe ids.size
    }
  }

  "Frame.hash (Fields, Field)" should {
    "add a Long hash column from the specified input columns" in {
      val df = Seq(("a", 1), ("b", 2)).toDF("s", "i")
      val out = df.frame.hash(("s", "i") -> Field("h")).df

      out.columns.toSeq shouldBe Seq("s", "i", "h")
      out.schema("h").dataType shouldBe LongType
    }

    "produce equal hashes for equal inputs" in {
      val df = Seq(("a", 1), ("a", 1)).toDF("s", "i")
      val hashes = df.frame.hash(("s", "i") -> Field("h")).df.select("h").as[Long].collect()
      hashes(0) shouldBe hashes(1)
    }

    "produce different hashes for different inputs" in {
      val df = Seq(("a", 1), ("b", 2)).toDF("s", "i")
      val hashes = df.frame.hash(("s", "i") -> Field("h")).df.select("h").as[Long].collect()
      hashes(0) should not be hashes(1)
    }

    "throw when input fields are empty" in {
      val df = Seq(("a", 1)).toDF("s", "i")
      val ex = intercept[IllegalArgumentException] {
        df.frame.hash(Fields.empty -> Field("h"))
      }
      ex.getMessage should include("hash requires at least one input field")
    }
  }

  "Frame.hash (Field)" should {
    "hash all columns into a single Long column" in {
      val df = Seq(("a", 1), ("b", 2)).toDF("s", "i")
      val out = df.frame.hash(Field("h")).df

      out.columns.toSeq shouldBe Seq("s", "i", "h")
      out.schema("h").dataType shouldBe LongType
    }

    "produce equal hashes for identical rows" in {
      val df = Seq(("a", 1), ("a", 1)).toDF("s", "i")
      val hashes = df.frame.hash(Field("h")).df.select("h").as[Long].collect()
      hashes(0) shouldBe hashes(1)
    }
  }

  "Frame.firstNonNull" should {
    "return the first non-null value across the input columns" in {
      val df = Seq[(String, String, String)](
        (null, "b", "c"),
        ("a", null, "c"),
        (null, null, "c")
      ).toDF("x", "y", "z")
      val out = df.frame.firstNonNull(("x", "y", "z") -> Field("first")).df

      out.orderBy("first").select("first").as[String].collect().toList shouldBe
        List("a", "b", "c")
    }

    "return null when all input columns are null" in {
      val df = Seq[(String, String)]((null, null)).toDF("x", "y")
      val out = df.frame.firstNonNull(("x", "y") -> Field("first")).df

      out.select("first").collect().toList shouldBe List(Row(null))
    }

    "throw when input fields are empty" in {
      val df = Seq("a").toDF("x")
      val ex = intercept[IllegalArgumentException] {
        df.frame.firstNonNull(Fields.empty -> Field("first"))
      }
      ex.getMessage should include("firstNonNull requires at least one input field")
    }
  }
}
