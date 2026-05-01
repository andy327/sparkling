package com.sparkling.evidence

import org.apache.spark.sql.{functions => sqlf}
import org.scalatest.matchers.should.Matchers._
import org.scalatest.wordspec.AnyWordSpec

import com.sparkling.testkit.SparkSuite

final class SparkPredicateEvidenceSpec extends AnyWordSpec with SparkSuite {
  import spark.implicits._

  "SparkPredicateEvidence" should {
    "provide evidence for Function1 and produce a working boolean UDF" in {
      val ev = implicitly[SparkPredicateEvidence[Int => Boolean]]
      val u = ev.udf((x: Int) => x > 1)

      u.deterministic shouldBe true

      val df = Seq(1, 2, 3).toDF("x")
      val out = df.withColumn("p", u(sqlf.col("x")))

      out.orderBy("x").select("p").collect().toList shouldBe List(
        org.apache.spark.sql.Row(false),
        org.apache.spark.sql.Row(true),
        org.apache.spark.sql.Row(true)
      )
    }

    "provide evidence for Function2 and produce a working boolean UDF" in {
      val ev = implicitly[SparkPredicateEvidence[(Int, Int) => Boolean]]
      val u = ev.udf((a: Int, b: Int) => a + b > 10)

      val df = Seq((1, 10), (2, 3)).toDF("a", "b")
      val out = df.withColumn("p", u(sqlf.col("a"), sqlf.col("b")))

      out.orderBy("a").select("p").collect().toList shouldBe List(
        org.apache.spark.sql.Row(true),
        org.apache.spark.sql.Row(false)
      )
    }

    "provide evidence for Function3 and produce a working boolean UDF" in {
      val ev = implicitly[SparkPredicateEvidence[(Int, Int, Int) => Boolean]]
      val u = ev.udf((a: Int, b: Int, c: Int) => a + b + c == 6)

      val df = Seq((1, 2, 3), (1, 2, 4)).toDF("a", "b", "c")
      val out = df.withColumn("p", u(sqlf.col("a"), sqlf.col("b"), sqlf.col("c")))

      out.orderBy("c").select("p").collect().toList shouldBe List(
        org.apache.spark.sql.Row(true),
        org.apache.spark.sql.Row(false)
      )
    }

    "provide evidence for Function4 and produce a working boolean UDF" in {
      val ev = implicitly[SparkPredicateEvidence[(Int, Int, Int, Int) => Boolean]]
      val u = ev.udf((a: Int, b: Int, c: Int, d: Int) => a < b && c < d)

      val df = Seq((1, 2, 3, 4), (2, 1, 3, 4)).toDF("a", "b", "c", "d")
      val out = df.withColumn("p", u(sqlf.col("a"), sqlf.col("b"), sqlf.col("c"), sqlf.col("d")))

      out.orderBy("a").select("p").collect().toList shouldBe List(
        org.apache.spark.sql.Row(true),
        org.apache.spark.sql.Row(false)
      )
    }

    "provide evidence for Function5 and produce a working boolean UDF" in {
      val ev = implicitly[SparkPredicateEvidence[(Int, Int, Int, Int, Int) => Boolean]]
      val u = ev.udf((a: Int, b: Int, c: Int, d: Int, e: Int) => a + b + c + d + e == 15)

      val df = Seq((1, 2, 3, 4, 5), (1, 2, 3, 4, 6)).toDF("a", "b", "c", "d", "e")
      val out = df.withColumn("p", u(sqlf.col("a"), sqlf.col("b"), sqlf.col("c"), sqlf.col("d"), sqlf.col("e")))

      out.orderBy("e").select("p").collect().toList shouldBe List(
        org.apache.spark.sql.Row(true),
        org.apache.spark.sql.Row(false)
      )
    }

    "return deterministic UDFs by default" in {
      val ev = implicitly[SparkPredicateEvidence[Int => Boolean]]
      val u = ev.udf((x: Int) => x > 0)

      u.deterministic shouldBe true
      u.asNondeterministic().deterministic shouldBe false
    }
  }
}
