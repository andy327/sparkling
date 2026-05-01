package com.sparkling.evidence

import org.apache.spark.sql.{functions => sqlf}
import org.scalatest.matchers.should.Matchers._
import org.scalatest.wordspec.AnyWordSpec

import com.sparkling.testkit.SparkSuite

final class SparkFunctionEvidenceSpec extends AnyWordSpec with SparkSuite {
  import spark.implicits._

  "SparkFunctionEvidence" should {
    "provide evidence for Function0 and produce a working UDF" in {
      val ev = implicitly[SparkFunctionEvidence[() => Int]]
      val u = ev.udf(() => 7)

      u.deterministic shouldBe true

      val df = Seq(1, 2, 3).toDF("x")
      val out = df.withColumn("y", u())

      out.orderBy("x").select("y").collect().toList shouldBe List(
        org.apache.spark.sql.Row(7),
        org.apache.spark.sql.Row(7),
        org.apache.spark.sql.Row(7)
      )
    }

    "provide evidence for Function1 and produce a working UDF" in {
      val ev = implicitly[SparkFunctionEvidence[Int => Int]]
      val u = ev.udf((x: Int) => x + 1)

      val df = Seq(1, 2, 3).toDF("x")
      val out = df.withColumn("y", u(sqlf.col("x")))

      out.orderBy("x").select("y").collect().toList shouldBe List(
        org.apache.spark.sql.Row(2),
        org.apache.spark.sql.Row(3),
        org.apache.spark.sql.Row(4)
      )
    }

    "provide evidence for Function2 and produce a working UDF" in {
      val ev = implicitly[SparkFunctionEvidence[(Int, Int) => Int]]
      val u = ev.udf((a: Int, b: Int) => a + b)

      val df = Seq((1, 10), (2, 20)).toDF("a", "b")
      val out = df.withColumn("y", u(sqlf.col("a"), sqlf.col("b")))

      out.orderBy("a").select("y").collect().toList shouldBe List(
        org.apache.spark.sql.Row(11),
        org.apache.spark.sql.Row(22)
      )
    }

    "provide evidence for Function3 and produce a working UDF" in {
      val ev = implicitly[SparkFunctionEvidence[(Int, Int, Int) => Int]]
      val u = ev.udf((a: Int, b: Int, c: Int) => a + b + c)

      val df = Seq((1, 10, 100), (2, 20, 200)).toDF("a", "b", "c")
      val out = df.withColumn("y", u(sqlf.col("a"), sqlf.col("b"), sqlf.col("c")))

      out.orderBy("a").select("y").collect().toList shouldBe List(
        org.apache.spark.sql.Row(111),
        org.apache.spark.sql.Row(222)
      )
    }

    "provide evidence for Function4 and produce a working UDF" in {
      val ev = implicitly[SparkFunctionEvidence[(Int, Int, Int, Int) => Int]]
      val u = ev.udf((a: Int, b: Int, c: Int, d: Int) => a + b + c + d)

      val df = Seq((1, 10, 100, 1000)).toDF("a", "b", "c", "d")
      val out = df.withColumn("y", u(sqlf.col("a"), sqlf.col("b"), sqlf.col("c"), sqlf.col("d")))

      out.select("y").head() shouldBe org.apache.spark.sql.Row(1111)
    }

    "provide evidence for Function5 and produce a working UDF" in {
      val ev = implicitly[SparkFunctionEvidence[(Int, Int, Int, Int, Int) => Int]]
      val u = ev.udf((a: Int, b: Int, c: Int, d: Int, e: Int) => a + b + c + d + e)

      val df = Seq((1, 10, 100, 1000, 10000)).toDF("a", "b", "c", "d", "e")
      val out = df.withColumn("y", u(sqlf.col("a"), sqlf.col("b"), sqlf.col("c"), sqlf.col("d"), sqlf.col("e")))

      out.select("y").head() shouldBe org.apache.spark.sql.Row(11111)
    }

    "return deterministic UDFs by default" in {
      val ev = implicitly[SparkFunctionEvidence[Int => Int]]
      val u = ev.udf((x: Int) => x + 1)

      u.deterministic shouldBe true
      u.asNondeterministic().deterministic shouldBe false
    }

    "support tuple-return UDFs (struct output) so fields can be extracted" in {
      val ev = implicitly[SparkFunctionEvidence[(Int, String) => (Int, String)]]
      val u = ev.udf((i: Int, s: String) => (i + 1, s + "!"))

      val df = Seq((1, "a"), (2, "b")).toDF("i", "s")
      val out = df.withColumn("t", u(sqlf.col("i"), sqlf.col("s")))

      val got =
        out
          .select(sqlf.col("t").getField("_1").as("i2"), sqlf.col("t").getField("_2").as("s2"))
          .orderBy("i2")
          .collect()
          .toList

      got shouldBe List(
        org.apache.spark.sql.Row(2, "a!"),
        org.apache.spark.sql.Row(3, "b!")
      )
    }

    "support exploding Seq outputs for Function0" in {
      val ev = implicitly[SparkFunctionEvidence.Aux[() => Seq[Int], Seq[Int]]]
      val u = ev.udfExplode[Int](() => Seq(7, 8))

      val df = Seq(1).toDF("x")
      val out = df.select(sqlf.explode(u()).as("y"))

      out.orderBy("y").collect().toList shouldBe List(
        org.apache.spark.sql.Row(7),
        org.apache.spark.sql.Row(8)
      )
    }

    "support exploding Array outputs for Function1" in {
      val ev = implicitly[SparkFunctionEvidence.Aux[Int => Array[Int], Array[Int]]]
      val u = ev.udfExplode[Int]((x: Int) => Array(x, x + 10))

      val df = Seq(1, 2).toDF("x")
      val out = df.select(sqlf.col("x"), sqlf.explode(u(sqlf.col("x"))).as("y"))

      out.orderBy("x", "y").collect().toList shouldBe List(
        org.apache.spark.sql.Row(1, 1),
        org.apache.spark.sql.Row(1, 11),
        org.apache.spark.sql.Row(2, 2),
        org.apache.spark.sql.Row(2, 12)
      )
    }

    "support exploding Option outputs for Function1 (Some/None)" in {
      val ev = implicitly[SparkFunctionEvidence.Aux[Int => Option[Int], Option[Int]]]
      val u = ev.udfExplode[Int]((x: Int) => if (x % 2 == 0) Some(x) else None)

      val df = Seq(1, 2, 3, 4).toDF("x")
      val out = df.select(sqlf.explode(u(sqlf.col("x"))).as("y"))

      out.orderBy("y").collect().toList shouldBe List(
        org.apache.spark.sql.Row(2),
        org.apache.spark.sql.Row(4)
      )
    }

    "support udfExplode for Function2" in {
      val ev = implicitly[SparkFunctionEvidence.Aux[(Int, Int) => Seq[Int], Seq[Int]]]
      val u = ev.udfExplode[Int]((a: Int, b: Int) => Seq(a + b))
      val df = Seq((1, 10), (2, 20)).toDF("a", "b")
      val out = df.select(sqlf.explode(u(sqlf.col("a"), sqlf.col("b"))).as("y"))
      out.orderBy("y").collect().toList shouldBe List(org.apache.spark.sql.Row(11), org.apache.spark.sql.Row(22))
    }

    "support udfExplode for Function3" in {
      val ev = implicitly[SparkFunctionEvidence.Aux[(Int, Int, Int) => Seq[Int], Seq[Int]]]
      val u = ev.udfExplode[Int]((a: Int, b: Int, c: Int) => Seq(a + b + c))
      val df = Seq((1, 10, 100)).toDF("a", "b", "c")
      val out = df.select(sqlf.explode(u(sqlf.col("a"), sqlf.col("b"), sqlf.col("c"))).as("y"))
      out.collect().toList shouldBe List(org.apache.spark.sql.Row(111))
    }

    "support udfExplode for Function4" in {
      val ev = implicitly[SparkFunctionEvidence.Aux[(Int, Int, Int, Int) => Seq[Int], Seq[Int]]]
      val u = ev.udfExplode[Int]((a: Int, b: Int, c: Int, d: Int) => Seq(a + b + c + d))
      val df = Seq((1, 10, 100, 1000)).toDF("a", "b", "c", "d")
      val out = df.select(sqlf.explode(u(sqlf.col("a"), sqlf.col("b"), sqlf.col("c"), sqlf.col("d"))).as("y"))
      out.collect().toList shouldBe List(org.apache.spark.sql.Row(1111))
    }

    "support udfExplode for Function5" in {
      val ev = implicitly[SparkFunctionEvidence.Aux[(Int, Int, Int, Int, Int) => Seq[Int], Seq[Int]]]
      val u = ev.udfExplode[Int]((a: Int, b: Int, c: Int, d: Int, e: Int) => Seq(a + b + c + d + e))
      val df = Seq((1, 10, 100, 1000, 10000)).toDF("a", "b", "c", "d", "e")
      val out =
        df.select(sqlf.explode(u(sqlf.col("a"), sqlf.col("b"), sqlf.col("c"), sqlf.col("d"), sqlf.col("e"))).as("y"))
      out.collect().toList shouldBe List(org.apache.spark.sql.Row(11111))
    }

    "treat null return values as empty for Seq/Array/Option" in {
      val evSeq = implicitly[SparkFunctionEvidence.Aux[Int => Seq[Int], Seq[Int]]]
      val uSeq = evSeq.udfExplode[Int]((x: Int) => if (x == 1) null else Seq(x))

      val evArr = implicitly[SparkFunctionEvidence.Aux[Int => Array[Int], Array[Int]]]
      val uArr = evArr.udfExplode[Int]((x: Int) => if (x == 1) null else Array(x))

      val evOpt = implicitly[SparkFunctionEvidence.Aux[Int => Option[Int], Option[Int]]]
      val uOpt = evOpt.udfExplode[Int]((x: Int) => if (x == 1) null else Some(x))

      val df = Seq(1, 2).toDF("x")

      df.select(sqlf.explode(uSeq(sqlf.col("x"))).as("y")).collect().toList shouldBe
        List(org.apache.spark.sql.Row(2))

      df.select(sqlf.explode(uArr(sqlf.col("x"))).as("y")).collect().toList shouldBe
        List(org.apache.spark.sql.Row(2))

      df.select(sqlf.explode(uOpt(sqlf.col("x"))).as("y")).collect().toList shouldBe
        List(org.apache.spark.sql.Row(2))
    }
  }
}
