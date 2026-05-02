package com.sparkling.frame

import org.apache.spark.sql.types._
import org.apache.spark.sql.{functions => sqlf, Row}
import org.scalatest.matchers.should.Matchers._
import org.scalatest.wordspec.AnyWordSpec

import com.sparkling.schema.Fields
import com.sparkling.syntax.FieldsSyntax._
import com.sparkling.syntax.FrameSyntax._
import com.sparkling.testkit.SparkSuite

final class FrameColumnShapeSpec extends AnyWordSpec with SparkSuite {
  import spark.implicits._

  "Frame.project" should {
    "keep only the requested columns in order" in {
      val df = Seq((1, "a", true)).toDF("x", "y", "z")
      val out = df.frame.project("y", "x").df

      out.columns.toSeq shouldBe Seq("y", "x")
      out.as[(String, Int)].collect().toSeq shouldBe Seq(("a", 1))
    }

    "accept a tuple via FieldsSyntax" in {
      val df = Seq((1, "a", true)).toDF("x", "y", "z")
      df.frame.project(("y", "x")).df.columns.toSeq shouldBe Seq("y", "x")
    }

    "accept a Seq[String] via FieldsSyntax" in {
      val df = Seq((1, "a", true)).toDF("x", "y", "z")
      df.frame.project(Seq("y", "x")).df.columns.toSeq shouldBe Seq("y", "x")
    }
  }

  "Frame.discard" should {
    "drop the requested columns" in {
      val df = Seq((1, "a", true)).toDF("x", "y", "z")
      val out = df.frame.discard("y").df

      out.columns.toSeq shouldBe Seq("x", "z")
    }

    "be a no-op when discarding zero columns" in {
      val df = Seq((1, "a")).toDF("x", "y")
      df.frame.discard(Fields.empty).df.columns.toSeq shouldBe Seq("x", "y")
    }
  }

  "Frame.rename" should {
    "rename a single column" in {
      val df = Seq((1, "a")).toDF("x", "y")
      val out = df.frame.rename("y" -> "yy").df

      out.columns.toSeq shouldBe Seq("x", "yy")
      out.as[(Int, String)].collect().toSeq shouldBe Seq((1, "a"))
    }

    "rename multiple columns" in {
      val df = Seq((1, "a")).toDF("x", "y")
      val out = df.frame.rename(("x", "y") -> ("xx", "yy")).df

      out.columns.toSeq shouldBe Seq("xx", "yy")
      out.as[(Int, String)].collect().toSeq shouldBe Seq((1, "a"))
    }

    "throw when arity differs" in {
      val df = Seq((1, "a")).toDF("x", "y")
      val ex = intercept[IllegalArgumentException] {
        df.frame.rename(("x", "y") -> "z")
      }
      ex.getMessage should include("rename requires same arity")
    }
  }

  "Frame.alias" should {
    "copy a single column under a new name while keeping the original" in {
      val df = Seq((1, "a")).toDF("x", "y")
      val out = df.frame.alias("y" -> "y2").df

      out.columns.toSeq shouldBe Seq("x", "y", "y2")
      out.select("y", "y2").as[(String, String)].collect().toSeq shouldBe Seq(("a", "a"))
    }

    "copy multiple columns under new names while keeping originals" in {
      val df = Seq((1, "a")).toDF("x", "y")
      val out = df.frame.alias(("x", "y") -> ("x2", "y2")).df

      out.columns.toSeq shouldBe Seq("x", "y", "x2", "y2")
      out.select("x2", "y2").as[(Int, String)].collect().toSeq shouldBe Seq((1, "a"))
    }

    "throw when arity differs" in {
      val df = Seq((1, "a")).toDF("x", "y")
      val ex = intercept[IllegalArgumentException] {
        df.frame.alias(("x", "y") -> "z")
      }
      ex.getMessage should include("alias requires same arity")
    }
  }

  "Frame.pack" should {
    "pack multiple columns into a single struct column" in {
      val df = Seq((1, "a")).toDF("i", "s")
      val out = df.frame.pack(("i", "s") -> "p").df

      out.columns.toSeq shouldBe Seq("i", "s", "p")

      val st = out.schema("p").dataType.asInstanceOf[StructType]
      st.fieldNames.toSeq shouldBe Seq("i", "s")

      out.select("p.i", "p.s").collect().toList shouldBe List(Row(1, "a"))
    }

    "throw when input fields are empty" in {
      val df = Seq((1, "a")).toDF("i", "s")
      val ex = intercept[IllegalArgumentException] {
        df.frame.pack(Fields.empty -> "p")
      }
      ex.getMessage should include("pack requires at least one input field")
    }
  }

  "Frame.unpack (Field, Fields)" should {
    "unpack a struct column into named output columns and drop the input" in {
      val df = Seq((1, "a")).toDF("i", "s")
      val out = df.frame.pack(("i", "s") -> "p").unpack("p" -> ("i", "s")).df

      out.columns.toSeq shouldBe Seq("i", "s")
      out.collect().toList shouldBe List(Row(1, "a"))
    }

    "unpack an array column into output columns by position and drop the input" in {
      val df = Seq(Seq(1, 2)).toDF("arr")
      val out = df.frame.unpack("arr" -> ("a", "b")).df

      out.columns.toSeq shouldBe Seq("a", "b")
      out.collect().toList shouldBe List(Row(1, 2))
    }

    "throw when output fields are empty" in {
      val df = Seq((1, "a")).toDF("i", "s")
      val packed = df.frame.pack(("i", "s") -> "p").df

      val ex = intercept[IllegalArgumentException] {
        Frame(packed).unpack("p" -> Fields.empty)
      }
      ex.getMessage should include("unpack requires at least one output field")
    }

    "throw when the column is neither StructType nor ArrayType" in {
      val df = Seq(1).toDF("x")
      val ex = intercept[IllegalArgumentException] {
        df.frame.unpack("x" -> ("a", "b"))
      }
      ex.getMessage should include("can only unpack a StructType or ArrayType")
    }
  }

  "Frame.unpack (Field)" should {
    "unpack a struct column using the struct's own field names and drop the input" in {
      val df = Seq((1, "a")).toDF("i", "s")
      val out = df.frame.pack(("i", "s") -> "p").unpack("p").df

      out.columns.toSeq shouldBe Seq("i", "s")
      out.collect().toList shouldBe List(Row(1, "a"))
    }

    "throw when the column is not a StructType" in {
      val df = Seq(1).toDF("x")
      val ex = intercept[IllegalArgumentException] {
        df.frame.unpack("x")
      }
      ex.getMessage should include("unpack(Field) requires a StructType column")
    }
  }

  "Frame.insert" should {
    "insert a constant value into one or more output columns" in {
      val df = Seq((1, "a"), (2, "b")).toDF("i", "s")
      val out = df.frame.insert(("x", "y"), 99).df

      out.columns.toSeq shouldBe Seq("i", "s", "x", "y")
      out.select("x", "y").as[(Int, Int)].collect().toSeq shouldBe Seq((99, 99), (99, 99))
    }

    "overwrite an existing column when the output field already exists" in {
      val df = Seq(1, 2).toDF("x")
      val out = df.frame.insert("x", 7).df

      out.columns.toSeq shouldBe Seq("x")
      out.as[Int].collect().toSeq shouldBe Seq(7, 7)
    }

    "throw when output fields are empty" in {
      val df = Seq(1).toDF("x")
      val ex = intercept[IllegalArgumentException] {
        df.frame.insert(Fields.empty, 1)
      }
      ex.getMessage should include("insert requires at least one output field")
    }
  }

  "Frame.cast" should {
    "cast input columns to output columns using a DataType" in {
      val df = Seq(("1", "2"), ("3", null)).toDF("a", "b")
      val out = df.frame.cast(("a", "b") -> ("ai", "bi"), IntegerType).df

      out.columns.toSeq shouldBe Seq("a", "b", "ai", "bi")
      out.select("ai", "bi").collect().toList shouldBe List(Row(1, 2), Row(3, null))
    }

    "cast a single column in place using the convenience overload" in {
      val df = Seq("1", "2", null).toDF("a")
      val out = df.frame.cast("a", IntegerType).df

      out.columns.toSeq shouldBe Seq("a")
      out.collect().toList shouldBe List(Row(1), Row(2), Row(null))
    }

    "throw when input fields are empty" in {
      val df = Seq(("1", "2")).toDF("a", "b")
      val ex = intercept[IllegalArgumentException] {
        df.frame.cast(Fields.empty -> "x", IntegerType)
      }
      ex.getMessage should include("cast requires at least one input field")
    }

    "throw when output fields are empty" in {
      val df = Seq(("1", "2")).toDF("a", "b")
      val ex = intercept[IllegalArgumentException] {
        df.frame.cast("a" -> Fields.empty, IntegerType)
      }
      ex.getMessage should include("cast requires at least one output field")
    }

    "throw when input/output arity differs" in {
      val df = Seq(("1", "2")).toDF("a", "b")
      val ex = intercept[IllegalArgumentException] {
        df.frame.cast(("a", "b") -> "x", IntegerType)
      }
      ex.getMessage should include("cast requires same arity")
    }
  }

  "Frame.withColumn" should {
    "add a new column from an expression" in {
      val df = Seq((1, "a"), (2, "b")).toDF("x", "y")
      val out = df.frame.withColumn("z", sqlf.col("x") + sqlf.lit(10)).df

      out.columns.toSeq shouldBe Seq("x", "y", "z")
      out.orderBy("x").select("z").collect().toList shouldBe List(Row(11), Row(12))
    }

    "overwrite an existing column" in {
      val df = Seq((1, "a"), (2, "b")).toDF("x", "y")
      val out = df.frame.withColumn("y", sqlf.lit("zz")).df

      out.columns.toSeq shouldBe Seq("x", "y")
      out.select("y").collect().toList shouldBe List(Row("zz"), Row("zz"))
    }
  }

  "Frame.withColumns (single expr)" should {
    "write a single output column from an expression" in {
      val df = Seq((1, "a"), (2, "b")).toDF("x", "y")
      val out = df.frame.withColumns(Fields("z"), sqlf.col("x") + sqlf.lit(10)).df

      out.columns.toSeq shouldBe Seq("x", "y", "z")
      out.orderBy("x").select("z").collect().toList shouldBe List(Row(11), Row(12))
    }

    "expand a struct expression into multiple output columns" in {
      val df = Seq((1, "a"), (2, "b")).toDF("x", "y")
      val expr = sqlf.struct(
        (sqlf.col("x") + sqlf.lit(1)).as("f1"),
        sqlf.concat(sqlf.col("y"), sqlf.lit("!")).as("f2")
      )
      val out = df.frame.withColumns(("x2", "y2"), expr).df

      out.columns.toSet shouldBe Set("x", "y", "x2", "y2")
      out.orderBy("x").select("x2", "y2").collect().toList shouldBe List(Row(2, "a!"), Row(3, "b!"))
    }

    "ignore extra struct fields when output arity is smaller than struct arity" in {
      val df = Seq((1, "a")).toDF("x", "y")
      val expr = sqlf.struct(
        (sqlf.col("x") + sqlf.lit(1)).as("f1"),
        sqlf.concat(sqlf.col("y"), sqlf.lit("!")).as("f2"),
        sqlf.lit("EXTRA").as("f3")
      )
      val out = df.frame.withColumns(("x2", "y2"), expr).df

      out.columns.toSet shouldBe Set("x", "y", "x2", "y2")
      out.select("x2", "y2").collect().toList shouldBe List(Row(2, "a!"))
    }

    "leave no temporary columns after struct expansion" in {
      val df = Seq((1, "a")).toDF("x", "y")
      val expr = sqlf.struct(sqlf.col("x").as("f1"), sqlf.col("y").as("f2"))
      val out = df.frame.withColumns(("x2", "y2"), expr).df

      out.columns.exists(_.startsWith("__tmp_")) shouldBe false
    }

    "throw when output fields are empty" in {
      val df = Seq((1, "a")).toDF("x", "y")
      val ex = intercept[IllegalArgumentException] {
        df.frame.withColumns(Fields.empty, sqlf.lit(1))
      }
      ex.getMessage should include("output fields must be non-empty")
    }

    "throw when multiple outputs are requested but expr is not a struct" in {
      val df = Seq((1, "a")).toDF("x", "y")
      val ex = intercept[IllegalArgumentException] {
        df.frame.withColumns(("a2", "b2"), sqlf.col("x") + sqlf.lit(1)).df.collect()
      }
      ex.getMessage should include("did not return a struct")
    }

    "throw when struct has fewer fields than requested outputs" in {
      val df = Seq((1, "a")).toDF("x", "y")
      val expr = sqlf.struct(sqlf.col("x").as("only"))
      val ex = intercept[IllegalArgumentException] {
        df.frame.withColumns(("a", "b", "c"), expr).df.collect()
      }
      ex.getMessage should include("output arity mismatch")
    }
  }

  "Frame.withColumns (Seq[Column])" should {
    "write multiple output columns from a sequence of expressions" in {
      val df = Seq((1, "a"), (2, "b")).toDF("x", "y")
      val out =
        df.frame
          .withColumns(("a", "b"), Seq(sqlf.lit(5), sqlf.col("x") * 3))
          .df

      out.columns.toSet shouldBe Set("x", "y", "a", "b")
      out.orderBy("x").select("a", "b").collect().toList shouldBe List(Row(5, 3), Row(5, 6))
    }

    "throw when output fields are empty" in {
      val df = Seq((1, "a")).toDF("x", "y")
      val ex = intercept[IllegalArgumentException] {
        df.frame.withColumns(Fields.empty, Seq(sqlf.lit(1)))
      }
      ex.getMessage should include("output fields must be non-empty")
    }

    "throw when cols.size != fields.size" in {
      val df = Seq((1, "a")).toDF("x", "y")
      val ex = intercept[IllegalArgumentException] {
        df.frame.withColumns(("a", "b"), Seq(sqlf.lit(1)))
      }
      ex.getMessage should include("output arity mismatch")
    }
  }
}
