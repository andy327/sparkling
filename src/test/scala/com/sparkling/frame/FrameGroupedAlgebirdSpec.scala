package com.sparkling.frame

import com.twitter.algebird.{Aggregator => AlbAggregator, Monoid, MonoidAggregator}
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.{Encoder, Encoders, Row}
import org.scalatest.matchers.should.Matchers._
import org.scalatest.wordspec.AnyWordSpec

import com.sparkling.algebird.BufferSerDe._
import com.sparkling.algebird.{Aggregators, AlgebirdAggregatorSyntax}
import com.sparkling.schema.{Field, Fields}
import com.sparkling.syntax.FieldsSyntax._
import com.sparkling.syntax.FrameSyntax._
import com.sparkling.testkit.SparkSuite

object FrameGroupedAlgebirdSpec {
  final case class SumMax(sum: Long, max: Int)
}

final class FrameGroupedAlgebirdSpec extends AnyWordSpec with SparkSuite {
  import FrameGroupedAlgebirdSpec._
  import spark.implicits._

  "GroupedFrame.minBy / maxBy" should {
    "return value at min/max order column per group" in {
      val df = Seq(
        ("a", "x", 3),
        ("a", "y", 1),
        ("b", "p", 10),
        ("b", "q", 2)
      ).toDF("k", "v", "ord")

      val out =
        df.frame
          .groupBy(Fields("k")) { g =>
            g.minBy(Field("v"), Field("ord"), Field("v_min"))
              .maxBy(Field("v"), Field("ord"), Field("v_max"))
          }
          .orderBy("k")
          .df

      out.columns.toSet shouldBe Set("k", "v_min", "v_max")
      out.collect().toList shouldBe List(
        Row("a", "y", "x"),
        Row("b", "q", "p")
      )
    }
  }

  "GroupedFrame.aggregate (Column)" should {
    "apply a raw SQL expression aggregation" in {
      val df = Seq(("a", 1), ("a", 2), ("b", 10)).toDF("k", "v")
      import org.apache.spark.sql.{functions => sqlf}

      val out =
        df.frame
          .groupBy(Fields("k")) { g =>
            g.aggregate(Fields("v") -> Fields("v_sum"))(sqlf.sum(sqlf.col("v")))
          }
          .orderBy("k")
          .df

      out.collect().toList shouldBe List(Row("a", 3L), Row("b", 10L))
    }

    "throw when output arity > 1" in {
      val df = Seq(("a", 1)).toDF("k", "v")
      import org.apache.spark.sql.{functions => sqlf}

      val ex = intercept[IllegalArgumentException] {
        df.frame.groupBy(Fields("k")) { g =>
          g.aggregate(Fields("v") -> Fields("x", "y"))(sqlf.sum(sqlf.col("v")))
        }
      }
      ex.getMessage should include("aggregate(Column) requires arity 1 output")
    }
  }

  "GroupedFrame.aggregate (SqlAggregator, struct input expansion)" should {
    "expand sub-fields of a single struct column into aggregator inputs" in {
      val df = Seq((3, 4), (1, 2)).toDF("a", "b")
        .select(
          org.apache.spark.sql.functions.struct(
            org.apache.spark.sql.functions.col("a"),
            org.apache.spark.sql.functions.col("b")
          ).as("pair")
        )

      object SumPair extends Aggregator[(Int, Int), Long, Long] {
        override def zero: Long = 0L
        override def reduce(b: Long, a: (Int, Int)): Long = b + a._1 + a._2
        override def merge(b1: Long, b2: Long): Long = b1 + b2
        override def finish(r: Long): Long = r
        override def bufferEncoder: Encoder[Long] = Encoders.scalaLong
        override def outputEncoder: Encoder[Long] = Encoders.scalaLong
      }

      val out =
        df.frame
          .groupAll { g =>
            g.aggregate(Fields("pair") -> Fields("total"))(SumPair)
          }
          .df

      out.select("total").as[Long].collect().head shouldBe 10L
    }
  }

  "GroupedFrame.aggregate (SqlAggregator)" should {
    "run a typed Spark aggregator with multi-column input" in {
      val df = Seq((3, 4), (1, 2)).toDF("a", "b")

      object SumPair extends Aggregator[(Int, Int), Long, Long] {
        override def zero: Long = 0L
        override def reduce(b: Long, a: (Int, Int)): Long = b + a._1 + a._2
        override def merge(b1: Long, b2: Long): Long = b1 + b2
        override def finish(r: Long): Long = r
        override def bufferEncoder: Encoder[Long] = Encoders.scalaLong
        override def outputEncoder: Encoder[Long] = Encoders.scalaLong
      }

      val out =
        df.frame
          .groupAll { g =>
            g.aggregate(Fields("a", "b") -> Fields("total"))(SumPair)
          }
          .df

      out.select("total").as[Long].collect().head shouldBe 10L
    }

    "run a typed Spark aggregator producing a Product output" in {
      val df = Seq(("a", 1), ("a", 3), ("b", 10)).toDF("k", "v")

      object SumAndMax extends Aggregator[Int, (Long, Int), SumMax] {
        override def zero: (Long, Int) = (0L, Int.MinValue)
        override def reduce(b: (Long, Int), a: Int): (Long, Int) = (b._1 + a, math.max(b._2, a))
        override def merge(b1: (Long, Int), b2: (Long, Int)): (Long, Int) =
          (b1._1 + b2._1, math.max(b1._2, b2._2))
        override def finish(r: (Long, Int)): SumMax = SumMax(r._1, r._2)
        override def bufferEncoder: Encoder[(Long, Int)] = Encoders.tuple(Encoders.scalaLong, Encoders.scalaInt)
        override def outputEncoder: Encoder[SumMax] = Encoders.product[SumMax]
      }

      val out =
        df.frame
          .groupBy(Fields("k")) { g =>
            g.aggregate(Fields("v") -> Fields("sm"))(SumAndMax)
          }
          .orderBy("k")
          .df

      val rows = out.select("k", "sm.sum", "sm.max").collect().toList
      rows shouldBe List(Row("a", 4L, 3), Row("b", 10L, 10))
    }
  }

  "GroupedFrame.aggregate (MonoidAggregator)" should {
    "run an Algebird MonoidAggregator via the SQL path (encodable buffer)" in {
      val df = Seq(("a", 1), ("a", 2), ("b", 10), ("c", 7)).toDF("k", "v")

      val sumLongAlg: MonoidAggregator[Int, Long, Long] =
        AlbAggregator.fromMonoid(Monoid.longMonoid, (x: Int) => x.toLong)

      val out =
        df.frame
          .repartition(8, Fields("k"))
          .groupBy(Fields("k")) { g =>
            g.aggregate(Fields("v") -> Fields("v_sum"))(sumLongAlg)
          }
          .repartition(4, Fields("k"))
          .orderBy("k")
          .df

      out.collect().toList shouldBe List(Row("a", 3L), Row("b", 10L), Row("c", 7L))
    }

    "run an Algebird MonoidAggregator via the Kryo/UDAF path (SpaceSaver top-k)" in {
      val df = Seq(
        ("a", "x"),
        ("a", "x"),
        ("a", "y"),
        ("a", "y"),
        ("b", "y"),
        ("b", "y"),
        ("b", "x")
      ).toDF("k", "v")

      val topKAlg = Aggregators.forSpaceSaver[String](capacity = 100, k = 2)

      val out =
        df.frame
          .repartition(4, Fields("k"))
          .groupBy(Fields("k")) { g =>
            g.aggregate(Fields("v") -> Fields("topk"))(topKAlg)
          }
          .repartition(2, Fields("k"))
          .orderBy("k")
          .df

      out.columns.toSeq shouldBe Seq("k", "topk")
      val rows = out.collect().toList.map { r =>
        val k = r.getString(0)
        val xs = r.getAs[scala.collection.Seq[Row]]("topk")
          .toVector.map(rr => rr.getString(0)).sorted
        (k, xs)
      }
      rows shouldBe List(("a", Vector("x", "y")), ("b", Vector("x", "y")))
    }

    "throw when Kryo-buffer MonoidAggregator is used with arity > 1 input" in {
      val df = Seq(("a", "x", "y")).toDF("k", "v1", "v2")
      val topKAlg = Aggregators.forSpaceSaver[String](capacity = 100, k = 2)

      val ex = intercept[IllegalArgumentException] {
        df.frame.groupBy(Fields("k")) { g =>
          g.aggregate(Fields("v1", "v2") -> Fields("topk"))(topKAlg)
        }
      }
      ex.getMessage should include("Kryo buffer requires arity 1 input")
    }
  }

  "GroupedFrame.aggregatePacked (Column)" should {
    "pack a SQL expression and unpack into multiple output columns" in {
      val df = Seq(("a", 1), ("a", 3), ("b", 10)).toDF("k", "v")
      import org.apache.spark.sql.{functions => sqlf}

      val packedExpr = sqlf.struct(
        sqlf.sum(sqlf.col("v")).as("v_sum"),
        sqlf.max(sqlf.col("v")).as("v_max")
      )

      val out =
        df.frame
          .groupBy(Fields("k")) { g =>
            g.aggregatePacked(Fields("v") -> Fields("v_sum", "v_max"))(packedExpr)
          }
          .orderBy("k")
          .df

      out.columns.toSet shouldBe Set("k", "v_sum", "v_max")
      out.collect().toList shouldBe List(Row("a", 4L, 3), Row("b", 10L, 10))
    }
  }

  "GroupedFrame.aggregatePacked (SqlAggregator)" should {
    "unpack a Product aggregator output into multiple columns" in {
      val df = Seq(("a", 1), ("a", 3), ("b", 10)).toDF("k", "v")

      object SumAndMax extends Aggregator[Int, (Long, Int), SumMax] {
        override def zero: (Long, Int) = (0L, Int.MinValue)
        override def reduce(b: (Long, Int), a: Int): (Long, Int) = (b._1 + a, math.max(b._2, a))
        override def merge(b1: (Long, Int), b2: (Long, Int)): (Long, Int) =
          (b1._1 + b2._1, math.max(b1._2, b2._2))
        override def finish(r: (Long, Int)): SumMax = SumMax(r._1, r._2)
        override def bufferEncoder: Encoder[(Long, Int)] = Encoders.tuple(Encoders.scalaLong, Encoders.scalaInt)
        override def outputEncoder: Encoder[SumMax] = Encoders.product[SumMax]
      }

      val out =
        df.frame
          .groupBy(Fields("k")) { g =>
            g.aggregatePacked(Fields("v") -> Fields("v_sum", "v_max"))(SumAndMax)
          }
          .orderBy("k")
          .df

      out.columns.toSet shouldBe Set("k", "v_sum", "v_max")
      out.collect().toList shouldBe List(Row("a", 4L, 3), Row("b", 10L, 10))
    }
  }

  "GroupedFrame.aggregatePacked (MonoidAggregator, SQL buffer)" should {
    "unpack a non-Kryo MonoidAggregator result into multiple columns" in {
      val df = Seq(("a", 1), ("a", 3), ("b", 10)).toDF("k", "v")

      val sumToSumMax: MonoidAggregator[Int, Long, SumMax] =
        AlbAggregator
          .fromMonoid(Monoid.longMonoid, (x: Int) => x.toLong)
          .andThenPresent(l => SumMax(l, l.toInt))

      val out =
        df.frame
          .groupBy(Fields("k")) { g =>
            g.aggregatePacked(Fields("v") -> Fields("sm_sum", "sm_max"))(sumToSumMax)
          }
          .orderBy("k")
          .df

      out.columns.toSet shouldBe Set("k", "sm_sum", "sm_max")
      out.collect().toList shouldBe List(Row("a", 4L, 4), Row("b", 10L, 10))
    }
  }

  "GroupedFrame.aggregatePacked (MonoidAggregator)" should {
    "unpack an Algebird aggregator with Kryo buffer into multiple columns" in {
      val df = Seq(("a", "x"), ("a", "x"), ("a", "y"), ("b", "z")).toDF("k", "v")

      val top1Alg = Aggregators.forSpaceSaver[String](capacity = 100, k = 1)
        .andThenPresent(_.headOption.map(r => (r.item, r.min, r.max)).orNull)

      val out =
        df.frame
          .groupBy(Fields("k")) { g =>
            g.aggregatePacked(Fields("v") -> Fields("item", "min_c", "max_c"))(top1Alg)
          }
          .orderBy("k")
          .df

      out.columns.toSet shouldBe Set("k", "item", "min_c", "max_c")
      val items = out.select("item").as[String].collect().toList
      items shouldBe List("x", "z")
    }
  }

  "GroupedFrame.aggregate (Column) guards" should {
    "throw when output is empty" in {
      val df = Seq(("a", 1)).toDF("k", "v")
      import org.apache.spark.sql.{functions => sqlf}
      val ex = intercept[IllegalArgumentException] {
        df.frame.groupBy(Fields("k")) { g =>
          g.aggregate(Fields("v") -> Fields.empty)(sqlf.sum(sqlf.col("v")))
        }
      }
      ex.getMessage should include("aggregate requires at least one output field")
    }
  }

  "GroupedFrame.aggregate (SqlAggregator) guards" should {
    "throw when input is empty" in {
      val df = Seq(("a", 1)).toDF("k", "v")
      object SumLong extends Aggregator[Int, Long, Long] {
        override def zero: Long = 0L
        override def reduce(b: Long, a: Int): Long = b + a
        override def merge(b1: Long, b2: Long): Long = b1 + b2
        override def finish(r: Long): Long = r
        override def bufferEncoder: Encoder[Long] = Encoders.scalaLong
        override def outputEncoder: Encoder[Long] = Encoders.scalaLong
      }
      val ex = intercept[IllegalArgumentException] {
        df.frame.groupBy(Fields("k")) { g =>
          g.aggregate(Fields.empty -> Fields("out"))(SumLong)
        }
      }
      ex.getMessage should include("aggregate requires at least one input field")
    }

    "throw when output is empty" in {
      val df = Seq(("a", 1)).toDF("k", "v")
      object SumLong extends Aggregator[Int, Long, Long] {
        override def zero: Long = 0L
        override def reduce(b: Long, a: Int): Long = b + a
        override def merge(b1: Long, b2: Long): Long = b1 + b2
        override def finish(r: Long): Long = r
        override def bufferEncoder: Encoder[Long] = Encoders.scalaLong
        override def outputEncoder: Encoder[Long] = Encoders.scalaLong
      }
      val ex = intercept[IllegalArgumentException] {
        df.frame.groupBy(Fields("k")) { g =>
          g.aggregate(Fields("v") -> Fields.empty)(SumLong)
        }
      }
      ex.getMessage should include("aggregate requires at least one output field")
    }

    "throw when output arity > 1" in {
      val df = Seq(("a", 1)).toDF("k", "v")
      object SumLong extends Aggregator[Int, Long, Long] {
        override def zero: Long = 0L
        override def reduce(b: Long, a: Int): Long = b + a
        override def merge(b1: Long, b2: Long): Long = b1 + b2
        override def finish(r: Long): Long = r
        override def bufferEncoder: Encoder[Long] = Encoders.scalaLong
        override def outputEncoder: Encoder[Long] = Encoders.scalaLong
      }
      val ex = intercept[IllegalArgumentException] {
        df.frame.groupBy(Fields("k")) { g =>
          g.aggregate(Fields("v") -> Fields("x", "y"))(SumLong)
        }
      }
      ex.getMessage should include("requires arity * -> 1 output")
    }
  }

  "GroupedFrame.aggregate (MonoidAggregator) guards" should {
    "throw when input is empty" in {
      val df = Seq(("a", 1)).toDF("k", "v")
      val sumLong: MonoidAggregator[Int, Long, Long] =
        AlbAggregator.fromMonoid(Monoid.longMonoid, (x: Int) => x.toLong)
      val ex = intercept[IllegalArgumentException] {
        df.frame.groupBy(Fields("k")) { g =>
          g.aggregate(Fields.empty -> Fields("out"))(sumLong)
        }
      }
      ex.getMessage should include("aggregate requires at least one input field")
    }

    "throw when output is empty" in {
      val df = Seq(("a", 1)).toDF("k", "v")
      val sumLong: MonoidAggregator[Int, Long, Long] =
        AlbAggregator.fromMonoid(Monoid.longMonoid, (x: Int) => x.toLong)
      val ex = intercept[IllegalArgumentException] {
        df.frame.groupBy(Fields("k")) { g =>
          g.aggregate(Fields("v") -> Fields.empty)(sumLong)
        }
      }
      ex.getMessage should include("aggregate requires at least one output field")
    }

    "throw when output arity > 1" in {
      val df = Seq(("a", 1)).toDF("k", "v")
      val sumLong: MonoidAggregator[Int, Long, Long] =
        AlbAggregator.fromMonoid(Monoid.longMonoid, (x: Int) => x.toLong)
      val ex = intercept[IllegalArgumentException] {
        df.frame.groupBy(Fields("k")) { g =>
          g.aggregate(Fields("v") -> Fields("x", "y"))(sumLong)
        }
      }
      ex.getMessage should include("aggregate(MonoidAggregator) requires arity * -> 1 output")
    }
  }

  "GroupedFrame.aggregatePacked (Column) guards" should {
    "throw when output is empty" in {
      val df = Seq(("a", 1)).toDF("k", "v")
      import org.apache.spark.sql.{functions => sqlf}
      val ex = intercept[IllegalArgumentException] {
        df.frame.groupBy(Fields("k")) { g =>
          g.aggregatePacked(Fields("v") -> Fields.empty)(sqlf.sum(sqlf.col("v")))
        }
      }
      ex.getMessage should include("aggregatePacked requires at least one output field")
    }
  }

  "GroupedFrame.aggregatePacked (SqlAggregator) guards" should {
    "throw when input is empty" in {
      val df = Seq(("a", 1)).toDF("k", "v")
      object SumLong extends Aggregator[Int, Long, Long] {
        override def zero: Long = 0L
        override def reduce(b: Long, a: Int): Long = b + a
        override def merge(b1: Long, b2: Long): Long = b1 + b2
        override def finish(r: Long): Long = r
        override def bufferEncoder: Encoder[Long] = Encoders.scalaLong
        override def outputEncoder: Encoder[Long] = Encoders.scalaLong
      }
      val ex = intercept[IllegalArgumentException] {
        df.frame.groupBy(Fields("k")) { g =>
          g.aggregatePacked(Fields.empty -> Fields("out"))(SumLong)
        }
      }
      ex.getMessage should include("aggregatePacked requires at least one input field")
    }

    "throw when output is empty" in {
      val df = Seq(("a", 1)).toDF("k", "v")
      object SumLong extends Aggregator[Int, Long, Long] {
        override def zero: Long = 0L
        override def reduce(b: Long, a: Int): Long = b + a
        override def merge(b1: Long, b2: Long): Long = b1 + b2
        override def finish(r: Long): Long = r
        override def bufferEncoder: Encoder[Long] = Encoders.scalaLong
        override def outputEncoder: Encoder[Long] = Encoders.scalaLong
      }
      val ex = intercept[IllegalArgumentException] {
        df.frame.groupBy(Fields("k")) { g =>
          g.aggregatePacked(Fields("v") -> Fields.empty)(SumLong)
        }
      }
      ex.getMessage should include("aggregatePacked requires at least one output field")
    }
  }

  "GroupedFrame.aggregatePacked (MonoidAggregator) guards" should {
    "throw when input is empty" in {
      val df = Seq(("a", "x")).toDF("k", "v")
      val topKAlg = Aggregators.forSpaceSaver[String](capacity = 100, k = 2)
      val ex = intercept[IllegalArgumentException] {
        df.frame.groupBy(Fields("k")) { g =>
          g.aggregatePacked(Fields.empty -> Fields("item"))(topKAlg)
        }
      }
      ex.getMessage should include("aggregatePacked requires at least one input field")
    }

    "throw when output is empty" in {
      val df = Seq(("a", "x")).toDF("k", "v")
      val topKAlg = Aggregators.forSpaceSaver[String](capacity = 100, k = 2)
      val ex = intercept[IllegalArgumentException] {
        df.frame.groupBy(Fields("k")) { g =>
          g.aggregatePacked(Fields("v") -> Fields.empty)(topKAlg)
        }
      }
      ex.getMessage should include("aggregatePacked requires at least one output field")
    }

    "throw when Kryo-buffer aggregator is used with arity > 1 input" in {
      val df = Seq(("a", "x", "y")).toDF("k", "v1", "v2")
      val topKAlg = Aggregators.forSpaceSaver[String](capacity = 100, k = 2)
      val ex = intercept[IllegalArgumentException] {
        df.frame.groupBy(Fields("k")) { g =>
          g.aggregatePacked(Fields("v1", "v2") -> Fields("item", "count"))(topKAlg)
        }
      }
      ex.getMessage should include("Kryo buffer requires arity 1 input")
    }
  }

  "AlgebirdAggregatorSyntax.monoidToSql" should {
    "adapt an Algebird Monoid into a reusable typed Aggregator" in {
      val df = Seq(("a", 1), ("a", 2), ("b", 10)).toDF("k", "v")
      val sumLong = AlgebirdAggregatorSyntax.monoidToSql[Int, Long](_.toLong)

      val out =
        df.frame
          .repartition(4, Fields("k"))
          .groupBy(Fields("k")) { g =>
            g.aggregate(Fields("v") -> Fields("v_sum"))(sumLong)
          }
          .repartition(2, Fields("k"))
          .orderBy("k")
          .df

      out.collect().toList shouldBe List(Row("a", 3L), Row("b", 10L))
    }
  }
}
