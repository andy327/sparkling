package com.sparkling.frame

import org.apache.spark.sql.{functions => sqlf, Row}
import org.scalatest.matchers.should.Matchers._
import org.scalatest.wordspec.AnyWordSpec

import com.sparkling.schema.Fields
import com.sparkling.syntax.FieldsSyntax._
import com.sparkling.syntax.FrameSyntax._
import com.sparkling.testkit.SparkSuite

final class FrameCleaningSpec extends AnyWordSpec with SparkSuite {
  import spark.implicits._

  "Frame.fillNulls (Double)" should {
    "replace nulls in specified columns with the given value" in {
      val df = Seq[(java.lang.Double, java.lang.Double)](
        (1.0, null),
        (null, 2.0)
      ).toDF("a", "b")
      val out = df.frame.fillNulls("a", 0.0: Double).df

      out.collect().toList shouldBe List(Row(1.0, null), Row(0.0, 2.0))
    }

    "throw when fields are empty" in {
      val df = Seq(1.0).toDF("a")
      val ex = intercept[IllegalArgumentException] {
        df.frame.fillNulls(Fields.empty, 0.0: Double)
      }
      ex.getMessage should include("fillNulls requires at least one field")
    }
  }

  "Frame.fillNulls (Long)" should {
    "replace nulls in specified columns with the given value" in {
      val df = Seq[(java.lang.Long, java.lang.Long)](
        (1L, null),
        (null, 2L)
      ).toDF("a", "b")
      val out = df.frame.fillNulls("a", 0L).df

      out.collect().toList shouldBe List(Row(1L, null), Row(0L, 2L))
    }

    "throw when fields are empty" in {
      val df = Seq(1L).toDF("a")
      val ex = intercept[IllegalArgumentException] {
        df.frame.fillNulls(Fields.empty, 0L)
      }
      ex.getMessage should include("fillNulls requires at least one field")
    }
  }

  "Frame.fillNulls (Boolean)" should {
    "replace nulls in specified columns with the given value" in {
      val df = Seq[(java.lang.Boolean, java.lang.Boolean)](
        (true, null),
        (null, false)
      ).toDF("a", "b")
      val out = df.frame.fillNulls("a", false).df

      out.collect().toList shouldBe List(Row(true, null), Row(false, false))
    }

    "throw when fields are empty" in {
      val df = Seq(true).toDF("a")
      val ex = intercept[IllegalArgumentException] {
        df.frame.fillNulls(Fields.empty, false)
      }
      ex.getMessage should include("fillNulls requires at least one field")
    }
  }

  "Frame.fillNulls (String)" should {
    "replace nulls in specified columns with the given value" in {
      val df = Seq[(String, String)](
        ("x", null),
        (null, "y")
      ).toDF("a", "b")
      val out = df.frame.fillNulls("a", "default").df

      out.collect().toList shouldBe List(Row("x", null), Row("default", "y"))
    }

    "throw when fields are empty" in {
      val df = Seq("x").toDF("a")
      val ex = intercept[IllegalArgumentException] {
        df.frame.fillNulls(Fields.empty, "default")
      }
      ex.getMessage should include("fillNulls requires at least one field")
    }
  }

  "Frame.replace" should {
    "replace matching string values in specified columns" in {
      val df = Seq(("foo", "bar"), ("baz", "foo")).toDF("a", "b")
      val out = df.frame.replace("a", Map("foo" -> "qux")).df

      out.collect().toList shouldBe List(Row("qux", "bar"), Row("baz", "foo"))
    }

    "throw when fields are empty" in {
      val df = Seq("foo").toDF("a")
      val ex = intercept[IllegalArgumentException] {
        df.frame.replace(Fields.empty, Map("foo" -> "bar"))
      }
      ex.getMessage should include("replace requires at least one field")
    }
  }

  "Frame.distinct (no-arg)" should {
    "remove fully duplicate rows" in {
      val df = Seq((1, "a"), (1, "a"), (2, "b")).toDF("x", "y")
      df.frame.distinct().count() shouldBe 2L
    }

    "be a no-op when all rows are already unique" in {
      val df = Seq((1, "a"), (2, "b")).toDF("x", "y")
      df.frame.distinct().count() shouldBe 2L
    }
  }

  "Frame.distinct (Fields)" should {
    "deduplicate on the specified columns only" in {
      val df = Seq((1, "a"), (1, "b"), (2, "a")).toDF("x", "y")
      df.frame.distinct("x").count() shouldBe 2L
    }
  }

  "Frame.dropNulls" should {
    "drop rows where any of the given columns is null" in {
      val df = Seq[(String, String)](("a", "b"), ("a", null), (null, "b"), (null, null)).toDF("x", "y")
      val out = df.frame.dropNulls(("x", "y")).df

      out.collect().toList shouldBe List(Row("a", "b"))
    }

    "only drop based on the specified columns, ignoring others" in {
      val df = Seq[(String, String)](("a", null), (null, "b")).toDF("x", "y")
      val out = df.frame.dropNulls("x").df

      out.collect().toList shouldBe List(Row("a", null))
    }

    "throw when fields are empty" in {
      val df = Seq("a").toDF("x")
      val ex = intercept[IllegalArgumentException] {
        df.frame.dropNulls(Fields.empty)
      }
      ex.getMessage should include("dropNulls requires at least one field")
    }
  }

  "Frame.dropAllNulls" should {
    "drop rows only when all of the given columns are null" in {
      val df = Seq[(String, String)](("a", "b"), ("a", null), (null, "b"), (null, null)).toDF("x", "y")
      val out = df.frame.dropAllNulls(("x", "y")).df

      out.collect().toList shouldBe List(Row("a", "b"), Row("a", null), Row(null, "b"))
    }

    "throw when fields are empty" in {
      val df = Seq("a").toDF("x")
      val ex = intercept[IllegalArgumentException] {
        df.frame.dropAllNulls(Fields.empty)
      }
      ex.getMessage should include("dropAllNulls requires at least one field")
    }
  }

  "Frame.filterNotNull" should {
    "drop rows where any of the given columns is null (delegates to dropNulls)" in {
      val df = Seq[(String, String)](("a", "b"), ("a", null), (null, null)).toDF("x", "y")
      val out = df.frame.filterNotNull(("x", "y")).df

      out.collect().toList shouldBe List(Row("a", "b"))
    }

    "throw when fields are empty" in {
      val df = Seq("a").toDF("x")
      val ex = intercept[IllegalArgumentException] {
        df.frame.filterNotNull(Fields.empty)
      }
      ex.getMessage should include("dropNulls requires at least one field")
    }
  }

  "Frame.where" should {
    "filter rows using a Column predicate" in {
      val df = Seq(1, 2, 3, 4).toDF("x")
      val out = df.frame.where(sqlf.col("x") > sqlf.lit(2)).df

      out.orderBy("x").as[Int].collect().toList shouldBe List(3, 4)
    }
  }

  "Frame.sample" should {
    "return a subset of rows" in {
      val df = spark.range(0, 1000).toDF("x")
      val n = df.frame.sample(0.1, seed = Some(42L)).count()

      n should be > 0L
      n should be < 1000L
    }

    "return a subset of rows without a seed" in {
      val df = spark.range(0, 1000).toDF("x")
      val n = df.frame.sample(0.1).count()

      n should be > 0L
      n should be < 1000L
    }

    "return the same rows for the same seed" in {
      val df = spark.range(0, 1000).toDF("x")
      val run1 = df.frame.sample(0.2, seed = Some(7L)).df.orderBy("x").as[Long].collect().toList
      val run2 = df.frame.sample(0.2, seed = Some(7L)).df.orderBy("x").as[Long].collect().toList

      run1 shouldBe run2
    }

    "support sampling with replacement" in {
      val df = spark.range(0, 100).toDF("x")
      // with replacement and a large fraction, row count can exceed original
      val n = df.frame.sample(2.0, withReplacement = true, seed = Some(1L)).count()
      n should be > 0L
    }

    "throw when fraction is negative" in {
      val df = Seq(1).toDF("x")
      val ex = intercept[IllegalArgumentException] {
        df.frame.sample(-0.1)
      }
      ex.getMessage should include("sample fraction must be non-negative")
    }

    "throw when fraction exceeds 1.0 without replacement" in {
      val df = Seq(1).toDF("x")
      val ex = intercept[IllegalArgumentException] {
        df.frame.sample(1.1)
      }
      ex.getMessage should include("sample fraction must be in [0.0, 1.0]")
    }
  }
}
