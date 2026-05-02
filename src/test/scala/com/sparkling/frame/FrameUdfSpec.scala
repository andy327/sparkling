package com.sparkling.frame

import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{functions => sqlf, Row => SparkRow}
import org.scalatest.matchers.should.Matchers._
import org.scalatest.wordspec.AnyWordSpec

import com.sparkling.schema.Fields
import com.sparkling.syntax.FieldsSyntax._
import com.sparkling.syntax.FrameSyntax._
import com.sparkling.testkit.SparkSuite

final class FrameUdfSpec extends AnyWordSpec with SparkSuite {
  import spark.implicits._

  "Frame.filter" should {
    "keep rows matching a single-column predicate" in {
      val df = Seq(1, 2, 3, 4).toDF("x")
      val out = df.frame.filter(Fields("x"))((x: Int) => x > 2).orderBy("x").df

      out.as[Int].collect().toList shouldBe List(3, 4)
    }

    "keep rows matching a two-column predicate" in {
      val df = Seq(("a", 1), ("", 2), ("b", 3)).toDF("s", "n")
      val out = df.frame.filter(Fields("s", "n"))((s: String, n: Int) => s.length > 0 && n > 0).df

      out.count() shouldBe 2L
    }

    "return empty frame when no rows match" in {
      val df = Seq(1, 2, 3).toDF("x")
      df.frame.filter(Fields("x"))((x: Int) => x > 100).count() shouldBe 0L
    }

    "throw when fields are empty" in {
      val df = Seq(1).toDF("x")
      val ex = intercept[IllegalArgumentException] {
        df.frame.filter(Fields.empty)((x: Int) => x > 0)
      }
      ex.getMessage should include("filter requires at least one input field")
    }
  }

  "Frame.map" should {
    "apply a single-column transform" in {
      val df = Seq(1, 2, 3).toDF("x")
      val out = df.frame.map(Fields("x") -> Fields("y"))((x: Int) => x * 10).orderBy("y").df

      out.select("y").as[Int].collect().toList shouldBe List(10, 20, 30)
    }

    "write to a new column, preserving the original" in {
      val df = Seq("hello", "world").toDF("s")
      val out = df.frame.map(Fields("s") -> Fields("n"))((s: String) => s.length).df

      (out.columns.toSeq should contain).allOf("s", "n")
    }

    "expand a two-input function into a single output column" in {
      val df = Seq((3, 4)).toDF("a", "b")
      val out = df.frame.map(Fields("a", "b") -> Fields("sum"))((a: Int, b: Int) => a + b).df

      out.select("sum").as[Int].collect().head shouldBe 7
    }

    "expand a function returning a tuple into multiple output columns" in {
      val df = Seq((3, 4)).toDF("a", "b")
      val out = df.frame
        .map(Fields("a", "b") -> Fields("s", "p"))((a: Int, b: Int) => (a + b, a * b))
        .df

      val row = out.select("s", "p").collect().head
      row.getInt(0) shouldBe 7
      row.getInt(1) shouldBe 12
    }

    "throw when input fields are empty" in {
      val df = Seq(1).toDF("x")
      val ex = intercept[IllegalArgumentException] {
        df.frame.map(Fields.empty -> Fields("y"))((x: Int) => x)
      }
      ex.getMessage should include("map requires at least one input field")
    }

    "throw when output fields are empty" in {
      val df = Seq(1).toDF("x")
      val ex = intercept[IllegalArgumentException] {
        df.frame.map(Fields("x") -> Fields.empty)((x: Int) => x)
      }
      ex.getMessage should include("map requires at least one output field")
    }
  }

  "Frame.flatMap" should {
    "expand a Seq return into multiple rows" in {
      val df = Seq("a b", "c").toDF("s")
      val out = df.frame
        .flatMap(Fields("s") -> Fields("w"))((s: String) => s.split(" ").toSeq)
        .orderBy("w")
        .df

      out.select("w").as[String].collect().toList shouldBe List("a", "b", "c")
    }

    "expand an Option return, dropping None rows" in {
      val df = Seq(1, 2, 3).toDF("x")
      val out = df.frame
        .flatMap(Fields("x") -> Fields("y"))((x: Int) => if (x % 2 == 0) Some(x) else None)
        .df

      out.select("y").as[Int].collect().toList shouldBe List(2)
    }

    "produce a null row for empty result when outer = true" in {
      val df = Seq(1, 2).toDF("x")
      val out = df.frame
        .flatMap(Fields("x") -> Fields("y"))((x: Int) => if (x == 1) Seq(x) else Seq.empty, outer = true)
        .df.orderBy(sqlf.col("y").asc_nulls_last)

      val rows = out.collect().toList
      rows.size shouldBe 2
      rows.head.getInt(1) shouldBe 1
      rows(1).isNullAt(1) shouldBe true
    }

    "throw when input fields are empty" in {
      val df = Seq(1).toDF("x")
      val ex = intercept[IllegalArgumentException] {
        df.frame.flatMap(Fields.empty -> Fields("y"))((x: Int) => Seq(x))
      }
      ex.getMessage should include("flatMap requires at least one input field")
    }

    "throw when output fields are empty" in {
      val df = Seq(1).toDF("x")
      val ex = intercept[IllegalArgumentException] {
        df.frame.flatMap(Fields("x") -> Fields.empty)((x: Int) => Seq(x))
      }
      ex.getMessage should include("flatMap requires at least one output field")
    }
  }

  "Frame.mapStruct" should {
    "apply a SparkRow => SparkRow transform over packed columns" in {
      val df = Seq((1, "a"), (2, "b")).toDF("n", "s")
      val outSchema = StructType(
        Seq(
          StructField("n2", IntegerType),
          StructField("s2", StringType)
        )
      )
      val out = df.frame
        .mapStruct(Fields("n", "s") -> Fields("n2", "s2"), outSchema) { row =>
          SparkRow(row.getInt(0) * 10, row.getString(1).toUpperCase)
        }
        .orderBy("n2")
        .df

      out.select("n2", "s2").collect().toList shouldBe
        List(SparkRow(10, "A"), SparkRow(20, "B"))
    }

    "throw when input fields are empty" in {
      val df = Seq(1).toDF("x")
      val outSchema = StructType(Seq(StructField("y", IntegerType)))
      val ex = intercept[IllegalArgumentException] {
        df.frame.mapStruct(Fields.empty -> Fields("y"), outSchema)(row => row)
      }
      ex.getMessage should include("mapStruct requires at least one input field")
    }

    "throw when output fields are empty" in {
      val df = Seq(1).toDF("x")
      val outSchema = StructType(Seq(StructField("y", IntegerType)))
      val ex = intercept[IllegalArgumentException] {
        df.frame.mapStruct(Fields("x") -> Fields.empty, outSchema)(row => row)
      }
      ex.getMessage should include("mapStruct requires at least one output field")
    }
  }

  "Frame.mapArrayStruct" should {
    "expand a row into multiple output rows" in {
      val df = Seq((1, 2)).toDF("a", "b")
      val outSchema = StructType(Seq(StructField("v", IntegerType)))
      val out = df.frame
        .mapArrayStruct(Fields("a", "b") -> Fields("v"), outSchema) { row =>
          val list = new java.util.ArrayList[SparkRow]()
          list.add(SparkRow(row.getInt(0)))
          list.add(SparkRow(row.getInt(1)))
          list
        }
        .orderBy("v")
        .df

      out.select("v").as[Int].collect().toList shouldBe List(1, 2)
    }

    "produce a null row when f returns null and outer = true" in {
      val df = Seq(1).toDF("x")
      val outSchema = StructType(Seq(StructField("y", IntegerType)))
      val out = df.frame
        .mapArrayStruct(Fields("x") -> Fields("y"), outSchema, outer = true)(_ => null)
        .df

      val rows = out.collect().toList
      rows.size shouldBe 1
      rows.head.isNullAt(1) shouldBe true
    }

    "throw when input fields are empty" in {
      val df = Seq(1).toDF("x")
      val outSchema = StructType(Seq(StructField("y", IntegerType)))
      val ex = intercept[IllegalArgumentException] {
        df.frame.mapArrayStruct(Fields.empty -> Fields("y"), outSchema)(_ => null)
      }
      ex.getMessage should include("mapArrayStruct requires at least one input field")
    }

    "throw when output fields are empty" in {
      val df = Seq(1).toDF("x")
      val outSchema = StructType(Seq(StructField("y", IntegerType)))
      val ex = intercept[IllegalArgumentException] {
        df.frame.mapArrayStruct(Fields("x") -> Fields.empty, outSchema)(_ => null)
      }
      ex.getMessage should include("mapArrayStruct requires at least one output field")
    }
  }

  "Frame.mapRecord" should {
    "transform a scalar column via RowDecoder/RowEncoder" in {
      val df = Seq(1, 2, 3).toDF("x")
      val out = df.frame
        .mapRecord[Int, String](Fields("x") -> Fields("s"))(x => x.toString)
        .orderBy("s")
        .df

      out.select("s").as[String].collect().toList shouldBe List("1", "2", "3")
    }

    "transform multiple input columns into multiple output columns" in {
      val df = Seq((1, 2), (3, 4)).toDF("a", "b")
      val out = df.frame
        .mapRecord[(Int, Int), (Int, Int)](Fields("a", "b") -> Fields("s", "p")) { case (a, b) =>
          (a + b, a * b)
        }
        .orderBy("s")
        .df

      out.select("s", "p").collect().toList shouldBe List(SparkRow(3, 2), SparkRow(7, 12))
    }

    "throw when input fields are empty" in {
      val df = Seq(1).toDF("x")
      val ex = intercept[IllegalArgumentException] {
        df.frame.mapRecord[Int, Int](Fields.empty -> Fields("y"))(x => x)
      }
      ex.getMessage should include("mapRecord requires at least one input field")
    }

    "throw when output fields are empty" in {
      val df = Seq(1).toDF("x")
      val ex = intercept[IllegalArgumentException] {
        df.frame.mapRecord[Int, Int](Fields("x") -> Fields.empty)(x => x)
      }
      ex.getMessage should include("mapRecord requires at least one output field")
    }
  }

  "Frame.flatMapRecord" should {
    "expand a scalar column into multiple rows" in {
      val df = Seq(3).toDF("n")
      val out = df.frame
        .flatMapRecord[Int, Int](Fields("n") -> Fields("v"))(n => (1 to n).toSeq)
        .orderBy("v")
        .df

      out.select("v").as[Int].collect().toList shouldBe List(1, 2, 3)
    }

    "drop rows when f returns an empty iterator" in {
      val df = Seq(1, 2, 3).toDF("x")
      val out = df.frame
        .flatMapRecord[Int, Int](Fields("x") -> Fields("y")) { x =>
          if (x % 2 == 0) Seq(x) else Seq.empty
        }
        .df

      out.select("y").as[Int].collect().toList shouldBe List(2)
    }

    "produce a null row for null/empty result when outer = true" in {
      val df = Seq(1, 2).toDF("x")
      val out = df.frame
        .flatMapRecord[Int, Int](Fields("x") -> Fields("y"), outer = true) { x =>
          if (x == 1) Seq(x) else Seq.empty
        }
        .df.orderBy(sqlf.col("y").asc_nulls_last)

      val rows = out.collect().toList
      rows.size shouldBe 2
      rows.head.getInt(1) shouldBe 1
      rows(1).isNullAt(1) shouldBe true
    }

    "drop rows when f returns null" in {
      val df = Seq(1, 2).toDF("x")
      val out = df.frame
        .flatMapRecord[Int, Int](Fields("x") -> Fields("y")) { _ =>
          null.asInstanceOf[IterableOnce[Int]]
        }
        .df

      out.count() shouldBe 0L
    }

    "throw when input fields are empty" in {
      val df = Seq(1).toDF("x")
      val ex = intercept[IllegalArgumentException] {
        df.frame.flatMapRecord[Int, Int](Fields.empty -> Fields("y"))(x => Seq(x))
      }
      ex.getMessage should include("flatMapRecord requires at least one input field")
    }

    "throw when output fields are empty" in {
      val df = Seq(1).toDF("x")
      val ex = intercept[IllegalArgumentException] {
        df.frame.flatMapRecord[Int, Int](Fields("x") -> Fields.empty)(x => Seq(x))
      }
      ex.getMessage should include("flatMapRecord requires at least one output field")
    }
  }
}
