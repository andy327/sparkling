package com.sparkling.frame

import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.{col, sum => sqlSum}
import org.apache.spark.sql.types.LongType
import org.scalatest.matchers.should.Matchers._
import org.scalatest.wordspec.AnyWordSpec

import com.sparkling.schema.{Field, Fields}
import com.sparkling.syntax.FieldsSyntax._
import com.sparkling.syntax.FrameSyntax._
import com.sparkling.testkit.SparkSuite

final class FrameGroupedSpec extends AnyWordSpec with SparkSuite {
  import spark.implicits._

  "Frame.groupBy" should {
    "throw when keys are empty" in {
      val df = Seq((1, "a")).toDF("x", "y")
      val ex = intercept[IllegalArgumentException] {
        df.frame.groupBy(Fields.empty)(_.count(Field("n")))
      }
      ex.getMessage should include("groupBy requires at least one key field")
    }

    "throw when no aggregations are planned" in {
      val df = Seq((1, "a")).toDF("x", "y")
      val ex = intercept[IllegalArgumentException] {
        df.frame.groupBy(Fields("x"))(identity)
      }
      ex.getMessage should include("no aggregations planned")
    }
  }

  "Frame.groupAll" should {
    "aggregate all rows into a single row" in {
      val df = Seq(1, 2, 3, 4).toDF("x")
      val out = df.frame.groupAll(_.count(Field("n"))).df

      out.count() shouldBe 1L
      out.select("n").as[Long].collect().head shouldBe 4L
    }

    "throw when no aggregations are planned" in {
      val df = Seq(1).toDF("x")
      val ex = intercept[IllegalArgumentException] {
        df.frame.groupAll(identity)
      }
      ex.getMessage should include("no aggregations planned")
    }
  }

  "GroupedFrame.count" should {
    "count rows per group" in {
      val df = Seq(("a", 1), ("a", 2), ("b", 3)).toDF("k", "v")
      val out = df.frame
        .groupBy(Fields("k"))(_.count(Field("n")))
        .orderBy("k")
        .df

      out.select("k", "n").collect().toList shouldBe List(Row("a", 2L), Row("b", 1L))
    }

    "produce a Long output column" in {
      val df = Seq(1, 2).toDF("x")
      val out = df.frame.groupAll(_.count(Field("n"))).df
      out.schema("n").dataType shouldBe LongType
    }
  }

  "GroupedFrame unary aggregation" should {
    "throw when input has more than one field" in {
      val df = Seq(("a", 1.0, 2.0)).toDF("k", "v1", "v2")
      val ex = intercept[IllegalArgumentException] {
        df.frame.groupBy(Fields("k"))(_.avg(Fields("v1", "v2") -> Field("mean")))
      }
      ex.getMessage should include("requires exactly one input field")
    }
  }

  "GroupedFrame.avg" should {
    "compute mean per group" in {
      val df = Seq(("a", 10.0), ("a", 20.0), ("b", 5.0)).toDF("k", "v")
      val out = df.frame
        .groupBy(Fields("k"))(_.avg(Fields("v") -> Field("avg_v")))
        .orderBy("k")
        .df

      val rows = out.select("k", "avg_v").collect().toList
      rows.head.getString(0) shouldBe "a"
      rows.head.getDouble(1) shouldBe 15.0 +- 1e-9
    }
  }

  "GroupedFrame.sum" should {
    "sum per group" in {
      val df = Seq(("a", 3), ("a", 4), ("b", 10)).toDF("k", "v")
      val out = df.frame
        .groupBy(Fields("k"))(_.sum(Fields("v") -> Field("total")))
        .orderBy("k")
        .df

      out.select("k", "total").collect().toList shouldBe
        List(Row("a", 7L), Row("b", 10L))
    }
  }

  "GroupedFrame.min" should {
    "find the minimum per group" in {
      val df = Seq(("a", 3), ("a", 1), ("b", 5)).toDF("k", "v")
      val out = df.frame
        .groupBy(Fields("k"))(_.min(Fields("v") -> Field("min_v")))
        .orderBy("k")
        .df

      out.select("k", "min_v").collect().toList shouldBe
        List(Row("a", 1), Row("b", 5))
    }
  }

  "GroupedFrame.max" should {
    "find the maximum per group" in {
      val df = Seq(("a", 3), ("a", 7), ("b", 2)).toDF("k", "v")
      val out = df.frame
        .groupBy(Fields("k"))(_.max(Fields("v") -> Field("max_v")))
        .orderBy("k")
        .df

      out.select("k", "max_v").collect().toList shouldBe
        List(Row("a", 7), Row("b", 2))
    }
  }

  "GroupedFrame.stddevPop" should {
    "compute population standard deviation" in {
      val df = Seq(("a", 2.0), ("a", 4.0)).toDF("k", "v")
      val out = df.frame
        .groupBy(Fields("k"))(_.stddevPop(Fields("v") -> Field("sd")))
        .df

      val sd = out.select("sd").as[Double].collect().head
      sd shouldBe 1.0 +- 1e-9
    }
  }

  "GroupedFrame.stddevSamp" should {
    "compute sample standard deviation" in {
      val df = Seq(("a", 2.0), ("a", 4.0)).toDF("k", "v")
      val out = df.frame
        .groupBy(Fields("k"))(_.stddevSamp(Fields("v") -> Field("sd")))
        .df

      val sd = out.select("sd").as[Double].collect().head
      // sample stddev of [2,4] = sqrt(2) ≈ 1.4142
      sd shouldBe math.sqrt(2.0) +- 1e-9
    }
  }

  "GroupedFrame.variancePop" should {
    "compute population variance" in {
      val df = Seq(("a", 2.0), ("a", 4.0)).toDF("k", "v")
      val out = df.frame
        .groupBy(Fields("k"))(_.variancePop(Fields("v") -> Field("vr")))
        .df

      val vr = out.select("vr").as[Double].collect().head
      vr shouldBe 1.0 +- 1e-9
    }
  }

  "GroupedFrame.varianceSamp" should {
    "compute sample variance" in {
      val df = Seq(("a", 2.0), ("a", 4.0)).toDF("k", "v")
      val out = df.frame
        .groupBy(Fields("k"))(_.varianceSamp(Fields("v") -> Field("vr")))
        .df

      val vr = out.select("vr").as[Double].collect().head
      vr shouldBe 2.0 +- 1e-9
    }
  }

  "GroupedFrame.countDistinct" should {
    "count distinct values per group" in {
      val df = Seq(("a", 1), ("a", 1), ("a", 2), ("b", 5)).toDF("k", "v")
      val out = df.frame
        .groupBy(Fields("k"))(_.countDistinct(Fields("v") -> Field("n")))
        .orderBy("k")
        .df

      out.select("k", "n").collect().toList shouldBe
        List(Row("a", 2L), Row("b", 1L))
    }
  }

  "GroupedFrame.approxCountDistinct" should {
    "throw when input has more than one field" in {
      val df = Seq(("a", 1, 2)).toDF("k", "v1", "v2")
      val ex = intercept[IllegalArgumentException] {
        df.frame.groupBy(Fields("k"))(_.approxCountDistinct(Fields("v1", "v2") -> Field("n")))
      }
      ex.getMessage should include("approxCountDistinct requires exactly one input field")
    }

    "approximate distinct count" in {
      val df = spark.range(0, 1000).toDF("x").withColumn("k", col("x") % 2)
      val out = df.frame
        .groupBy(Fields("k"))(_.approxCountDistinct(Fields("x") -> Field("n")))
        .df

      out.count() shouldBe 2L
      out.select("n").as[Long].collect().foreach(_ should be > 0L)
    }
  }

  "GroupedFrame.approxPercentile" should {
    "throw when input has more than one field" in {
      val df = Seq(("a", 1.0, 2.0)).toDF("k", "v1", "v2")
      val ex = intercept[IllegalArgumentException] {
        df.frame.groupBy(Fields("k"))(_.approxPercentile(Fields("v1", "v2") -> Field("p"), Seq(0.5)))
      }
      ex.getMessage should include("approxPercentile requires exactly one input field")
    }

    "compute a single percentile" in {
      val df = (1 to 100).map(i => ("a", i.toDouble)).toDF("k", "v")
      val out = df.frame
        .groupBy(Fields("k"))(_.approxPercentile(Fields("v") -> Field("p50"), Seq(0.5)))
        .df

      val p50 = out.select("p50").as[Double].collect().head
      p50 shouldBe 50.0 +- 5.0
    }

    "compute multiple percentiles as an array" in {
      val df = (1 to 100).map(i => ("a", i.toDouble)).toDF("k", "v")
      val out = df.frame
        .groupBy(Fields("k"))(_.approxPercentile(Fields("v") -> Field("ps"), Seq(0.25, 0.75)))
        .df

      out.count() shouldBe 1L
    }

    "throw when percentage list is empty" in {
      val df = Seq(("a", 1.0)).toDF("k", "v")
      val ex = intercept[IllegalArgumentException] {
        df.frame.groupBy(Fields("k"))(_.approxPercentile(Fields("v") -> Field("p"), Seq.empty))
      }
      ex.getMessage should include("at least one percentage value")
    }
  }

  "GroupedFrame.first" should {
    "throw when input has more than one field" in {
      val df = Seq(("a", 1, 2)).toDF("k", "v1", "v2")
      val ex = intercept[IllegalArgumentException] {
        df.frame.groupBy(Fields("k"))(_.first(Fields("v1", "v2") -> Field("fv")))
      }
      ex.getMessage should include("first requires exactly one input field")
    }

    // Spark's first() is non-deterministic within a partition, so we only assert group count.
    "return the first value per group" in {
      val df = Seq(("a", 1), ("a", 2), ("b", 3)).toDF("k", "v")
      val out = df.frame
        .groupBy(Fields("k"))(_.first(Fields("v") -> Field("fv")))
        .df

      out.count() shouldBe 2L
    }

    "skip nulls when ignoreNulls = true" in {
      val df = spark.createDataFrame(
        spark.sparkContext.parallelize(
          Seq(
            Row("a", null: java.lang.Integer),
            Row("a", 2: java.lang.Integer)
          )
        ),
        org.apache.spark.sql.types.StructType(
          Seq(
            org.apache.spark.sql.types.StructField("k", org.apache.spark.sql.types.StringType),
            org.apache.spark.sql.types.StructField("v", org.apache.spark.sql.types.IntegerType)
          )
        )
      )
      val out = df.frame
        .groupBy(Fields("k"))(_.first(Fields("v") -> Field("fv"), ignoreNulls = true))
        .df

      out.select("fv").collect().head.getInt(0) shouldBe 2
    }
  }

  "GroupedFrame.last" should {
    "throw when input has more than one field" in {
      val df = Seq(("a", 1, 2)).toDF("k", "v1", "v2")
      val ex = intercept[IllegalArgumentException] {
        df.frame.groupBy(Fields("k"))(_.last(Fields("v1", "v2") -> Field("lv")))
      }
      ex.getMessage should include("last requires exactly one input field")
    }

    // Spark's last() is non-deterministic within a partition, so we only assert group count.
    "return the last value per group" in {
      val df = Seq(("a", 1), ("a", 2), ("b", 3)).toDF("k", "v")
      val out = df.frame
        .groupBy(Fields("k"))(_.last(Fields("v") -> Field("lv")))
        .df

      out.count() shouldBe 2L
    }
  }

  "GroupedFrame.collectList" should {
    "collect all values into an array per group" in {
      val df = Seq(("a", 1), ("a", 2), ("b", 3)).toDF("k", "v")
      val out = df.frame
        .groupBy(Fields("k"))(_.collectList(Fields("v") -> Field("vs")))
        .orderBy("k")
        .df

      val rows = out.select("vs").collect().map(_.getSeq[Int](0).sorted)
      rows.toList shouldBe List(Seq(1, 2), Seq(3))
    }
  }

  "GroupedFrame.collectSet" should {
    "collect distinct values per group" in {
      val df = Seq(("a", 1), ("a", 1), ("a", 2), ("b", 3)).toDF("k", "v")
      val out = df.frame
        .groupBy(Fields("k"))(_.collectSet(Fields("v") -> Field("vs")))
        .orderBy("k")
        .df

      val rows = out.select("vs").collect().map(_.getSeq[Int](0).sorted)
      rows.toList shouldBe List(Seq(1, 2), Seq(3))
    }
  }

  "GroupedFrame.expr" should {
    "accept an arbitrary aggregation expression" in {
      val df = Seq(("a", 3), ("a", 7)).toDF("k", "v")
      val out = df.frame
        .groupBy(Fields("k"))(_.expr(Field("total"), sqlSum(col("v"))))
        .df

      out.select("total").as[Long].collect().head shouldBe 10L
    }
  }

  "GroupedFrame.exprs" should {
    "accept multiple (Field, Column) pairs" in {
      val df = Seq(("a", 3), ("a", 7)).toDF("k", "v")
      val out = df.frame
        .groupBy(Fields("k"))(
          _.exprs(
            Seq(
              Field("total") -> sqlSum(col("v")),
              Field("n") -> org.apache.spark.sql.functions.count(col("v"))
            )
          )
        )
        .df

      val row = out.select("total", "n").collect().head
      row.getLong(0) shouldBe 10L
      row.getLong(1) shouldBe 2L
    }

    "throw when exprs list is empty" in {
      val df = Seq(("a", 1)).toDF("k", "v")
      val ex = intercept[IllegalArgumentException] {
        df.frame.groupBy(Fields("k"))(_.exprs(Seq.empty))
      }
      ex.getMessage should include("at least one aggregation")
    }
  }

  "GroupedFrame.pivot" should {
    "pivot on a column with inferred values" in {
      val df = Seq(("a", "x", 1), ("a", "y", 2), ("b", "x", 3)).toDF("k", "p", "v")
      val out = df.frame
        .groupBy(Fields("k"))(
          _.pivot(Field("p"))
            .sum(Fields("v") -> Field("sum_v"))
        )
        .orderBy("k")
        .df

      (out.columns.toSeq should contain).allOf("k", "x", "y")
      val rows = out.select("k", "x", "y").collect().toList
      rows(0).getString(0) shouldBe "a"
      rows(0).getLong(1) shouldBe 1L
      rows(0).getLong(2) shouldBe 2L
      rows(1).getString(0) shouldBe "b"
      rows(1).getLong(1) shouldBe 3L
      rows(1).isNullAt(2) shouldBe true
    }

    "pivot on a column with enumerated values" in {
      val df = Seq(("a", "x", 1), ("a", "y", 2), ("b", "x", 3)).toDF("k", "p", "v")
      val out = df.frame
        .groupBy(Fields("k"))(
          _.pivot(Field("p"), Fields("x", "y"))
            .sum(Fields("v") -> Field("sum_v"))
        )
        .orderBy("k")
        .df

      (out.columns.toSeq should contain).allOf("k", "x", "y")
      val rows = out.select("k", "x", "y").collect().toList
      rows(0).getString(0) shouldBe "a"
      rows(0).getLong(1) shouldBe 1L
      rows(0).getLong(2) shouldBe 2L
    }

    "throw when pivot is called more than once" in {
      val df = Seq(("a", "x", 1)).toDF("k", "p", "v")
      val ex = intercept[IllegalArgumentException] {
        df.frame.groupBy(Fields("k"))(
          _.pivot(Field("p"))
            .pivot(Field("p"))
            .count(Field("n"))
        )
      }
      ex.getMessage should include("pivot may only be called once")
    }
  }

  "GroupedFrame.postMap" should {
    "apply a transformation to the aggregated frame" in {
      val df = Seq(("a", 1), ("a", 2), ("b", 3)).toDF("k", "v")
      val out = df.frame
        .groupBy(Fields("k")) {
          _.count(Field("n"))
            .postMap(_.where(col("n") > org.apache.spark.sql.functions.lit(1)))
        }
        .df

      out.count() shouldBe 1L
      out.select("k").as[String].collect().head shouldBe "a"
    }

    "compose multiple postMaps in order" in {
      val df = Seq(("a", 1), ("b", 2), ("c", 3)).toDF("k", "v")
      val out = df.frame
        .groupBy(Fields("k")) {
          _.sum(Fields("v") -> Field("total"))
            .postMap(_.where(col("total") > org.apache.spark.sql.functions.lit(1)))
            .postMap(_.orderBy(Fields("k")))
        }
        .df

      out.select("k").as[String].collect().toList shouldBe List("b", "c")
    }
  }

  "GroupedFrame (multiple aggs)" should {
    "compute count, sum, and avg in one pass" in {
      val df = Seq(("a", 2.0), ("a", 4.0), ("b", 10.0)).toDF("k", "v")
      val out = df.frame
        .groupBy(Fields("k")) {
          _.count(Field("n"))
            .sum(Fields("v") -> Field("total"))
            .avg(Fields("v") -> Field("mean"))
        }
        .orderBy("k")
        .df

      val rows = out.select("k", "n", "total", "mean").collect().toList
      rows.head.getString(0) shouldBe "a"
      rows.head.getLong(1) shouldBe 2L
      rows.head.getDouble(2) shouldBe 6.0 +- 1e-9
      rows.head.getDouble(3) shouldBe 3.0 +- 1e-9
    }
  }
}
