package com.sparkling.frame

import org.apache.spark.sql.Row
import org.apache.spark.storage.StorageLevel
import org.scalatest.matchers.should.Matchers._
import org.scalatest.wordspec.AnyWordSpec

import com.sparkling.schema.{Field, Fields}
import com.sparkling.syntax.FieldsSyntax._
import com.sparkling.syntax.FrameSyntax._
import com.sparkling.testkit.SparkSuite

final class FrameSpec extends AnyWordSpec with SparkSuite {
  import spark.implicits._

  "Frame.schema" should {
    "return the underlying Spark schema" in {
      val df = Seq((1, "a")).toDF("x", "y")
      df.frame.schema.fieldNames.toSeq shouldBe Seq("x", "y")
    }
  }

  "Frame.columns" should {
    "return the underlying column names" in {
      val df = Seq((1, "a", true)).toDF("x", "y", "z")
      df.frame.columns shouldBe Seq("x", "y", "z")
    }
  }

  "Frame.col" should {
    "access a column on the underlying DataFrame" in {
      val df = Seq((1, "a")).toDF("x", "y")
      val out = df.select(df.frame.col("y")).as[String].collect().toSeq
      out shouldBe Seq("a")
    }
  }

  "Frame.hasFields" should {
    "return true when all requested fields exist" in {
      val df = Seq((1, "a", true)).toDF("x", "y", "z")
      df.frame.hasFields("x", "z") shouldBe true
    }

    "return false when any requested field is missing" in {
      val df = Seq((1, "a")).toDF("x", "y")
      df.frame.hasFields("x", "missing") shouldBe false
    }
  }

  "Frame.isEmpty / nonEmpty" should {
    "return true/false correctly for empty and non-empty DataFrames" in {
      val empty = spark.emptyDataset[Int].toDF("x")
      empty.frame.isEmpty shouldBe true
      empty.frame.nonEmpty shouldBe false

      val nonEmpty = Seq(1).toDF("x")
      nonEmpty.frame.isEmpty shouldBe false
      nonEmpty.frame.nonEmpty shouldBe true
    }
  }

  "Frame.thenDo" should {
    "apply f to this Frame and return the result" in {
      val df = Seq((1, "a"), (2, "b")).toDF("x", "y")

      df.frame.thenDo(_.columns) shouldBe Seq("x", "y")
      df.frame.thenDo(_.count()) shouldBe 2L
    }
  }

  "Frame.maybeDo (Option)" should {
    "apply f when the option is Some" in {
      val df = Seq((1, "a")).toDF("x", "y")

      val cols =
        df.frame
          .maybeDo(Some(3))((fr, n) => Frame(fr.df.limit(n)))
          .columns

      cols shouldBe Seq("x", "y")
    }

    "be a no-op when the option is None" in {
      val df = Seq((1, "a")).toDF("x", "y")

      val out =
        df.frame
          .maybeDo(Option.empty[Int])((fr, n) => Frame(fr.df.limit(n)))
          .df

      out.collect().toList shouldBe List(Row(1, "a"))
    }
  }

  "Frame.maybeDo (Boolean)" should {
    "apply f when the condition is true" in {
      val df = Seq((1, "a"), (2, "b")).toDF("x", "y")
      df.frame.maybeDo(true)(_.limit(1)).count() shouldBe 1L
    }

    "be a no-op when the condition is false" in {
      val df = Seq((1, "a"), (2, "b")).toDF("x", "y")
      df.frame.maybeDo(false)(_.limit(1)).count() shouldBe 2L
    }
  }

  "Frame.count" should {
    "return the number of rows" in {
      val df = Seq(1, 2, 3).toDF("x")
      df.frame.count() shouldBe 3L
    }
  }

  "Frame.printSchema" should {
    "not throw" in {
      val df = Seq((1, "a")).toDF("x", "y")
      noException shouldBe thrownBy(df.frame.printSchema())
    }
  }

  "Frame.printSchemaAndReturn" should {
    "print the schema and return the same Frame instance for chaining" in {
      val df = Seq((1, "a")).toDF("x", "y")
      val fr = df.frame

      noException shouldBe thrownBy(fr.printSchemaAndReturn())
      (fr.printSchemaAndReturn() eq fr) shouldBe true
    }
  }

  "Frame.show" should {
    "not throw" in {
      val df = Seq((1, "a")).toDF("x", "y")
      noException shouldBe thrownBy(df.frame.show(numRows = 5, truncate = false))
    }
  }

  "Frame.showAndReturn" should {
    "show rows and return the same Frame instance for chaining" in {
      val df = Seq((1, "a")).toDF("x", "y")
      val fr = df.frame

      noException shouldBe thrownBy(fr.showAndReturn())
      (fr.showAndReturn() eq fr) shouldBe true
    }
  }

  "Frame.explain" should {
    "not throw" in {
      val df = Seq((1, "a")).toDF("x", "y")
      noException shouldBe thrownBy(df.frame.explain(extended = false))
    }
  }

  "Frame.cache" should {
    "return a cached DataFrame without changing data" in {
      val df = Seq((1, "a"), (2, "b")).toDF("x", "y")
      val out = df.frame.cache().df

      out.storageLevel should not be StorageLevel.NONE
      out.orderBy("x").collect().toList shouldBe List(Row(1, "a"), Row(2, "b"))
    }
  }

  "Frame.persist" should {
    "persist with a given storage level" in {
      spark.catalog.clearCache()

      val df = Seq((1, "a"), (2, "b")).toDF("x", "y")
      val out = df.frame.persist(StorageLevel.MEMORY_ONLY).df

      out.storageLevel shouldBe StorageLevel.MEMORY_ONLY
      out.count() shouldBe 2L

      out.unpersist(blocking = false)
    }

    "materialize immediately when chained with materialize()" in {
      spark.catalog.clearCache()

      val df = Seq((1, "a"), (2, "b")).toDF("x", "y")
      val out = df.frame.persist(StorageLevel.MEMORY_ONLY).materialize().df

      out.storageLevel shouldBe StorageLevel.MEMORY_ONLY
      out.count() shouldBe 2L

      out.unpersist(blocking = false)
    }
  }

  "Frame.unpersist" should {
    "unpersist a cached DataFrame" in {
      val df = Seq((1, "a"), (2, "b")).toDF("x", "y")
      val cached = df.frame.cache().df
      cached.storageLevel should not be StorageLevel.NONE

      val out = Frame(cached).unpersist(blocking = false).df
      out.storageLevel shouldBe StorageLevel.NONE
    }
  }

  "Frame.materialize" should {
    "trigger an action and return the same Frame instance for chaining" in {
      val df = Seq((1, "a"), (2, "b")).toDF("x", "y")
      val fr = df.frame.cache()

      val out = fr.materialize()
      (out eq fr) shouldBe true
      out.df.count() shouldBe 2L
    }
  }

  "Frame.limit" should {
    "limit the number of rows" in {
      val df = spark.range(0, 10).toDF("x")
      df.frame.limit(3).count() shouldBe 3L
    }
  }

  "Frame.take" should {
    "project the requested fields, decode as T, and return an IndexedSeq" in {
      val df = Seq((1, "a"), (2, "b"), (3, "c")).toDF("i", "s")
      val got: IndexedSeq[(String, Int)] =
        df.orderBy("i").frame.take[(String, Int)](("s", "i"), num = 2)
      got shouldBe IndexedSeq(("a", 1), ("b", 2))
    }

    "return fewer than num rows when the DataFrame is smaller" in {
      val df = Seq((1, "a")).toDF("i", "s")
      val got: IndexedSeq[(Int, String)] = df.frame.take[(Int, String)](("i", "s"), num = 10)
      got shouldBe IndexedSeq((1, "a"))
    }

    "throw when fields are empty" in {
      val df = Seq((1, "a")).toDF("i", "s")
      val ex = intercept[IllegalArgumentException] {
        df.frame.take[(Int, String)](Fields.empty, num = 1)
      }
      ex.getMessage should include("take requires at least one field")
    }
  }

  "Frame.repartition" should {
    "repartition to a fixed number of partitions" in {
      val df = spark.range(0, 100).toDF("x")
      df.frame.repartition(7).df.rdd.getNumPartitions shouldBe 7
      df.frame.repartition(7).count() shouldBe 100L
    }

    "repartition by key columns without changing row count" in {
      val df = Seq((1, "a"), (1, "b"), (2, "c")).toDF("k", "v")
      df.frame.repartition("k").count() shouldBe 3L
    }

    "repartition to a fixed number of partitions using key columns" in {
      val df = Seq((1, "a"), (2, "b"), (3, "c")).toDF("k", "v")
      val out = df.frame.repartition(5, "k").df
      out.rdd.getNumPartitions shouldBe 5
      out.count() shouldBe 3L
    }
  }

  "Frame.coalesce" should {
    "coalesce to a smaller number of partitions" in {
      val df = spark.range(0, 100).toDF("x").repartition(10)
      val out = df.frame.coalesce(3).df
      out.rdd.getNumPartitions shouldBe 3
      out.count() shouldBe 100L
    }
  }

  "Frame.sortWithinPartitions" should {
    "sort within partitions" in {
      val df = Seq(3, 1, 2).toDF("x").coalesce(1)
      df.frame.sortWithinPartitions("x").df.as[Int].collect().toList shouldBe List(1, 2, 3)
    }

    "sort within partitions ascending and descending" in {
      val df = Seq(3, 1, 2).toDF("x").repartition(1)

      val asc = df.frame.sortWithinPartitions("x").df.collect().toList
      asc shouldBe List(Row(1), Row(2), Row(3))

      val desc = df.frame.sortWithinPartitions("x", descending = true).df.collect().toList
      desc shouldBe List(Row(3), Row(2), Row(1))
    }

    "throw when fields are empty" in {
      val df = Seq((1, "a")).toDF("x", "y")
      val ex = intercept[IllegalArgumentException] {
        df.frame.sortWithinPartitions(Fields.empty)
      }
      ex.getMessage should include("sortWithinPartitions requires at least one field")
    }
  }

  "Frame.orderBy (Fields)" should {
    "order by fields ascending by default" in {
      val df = Seq(3, 1, 2).toDF("x")
      df.frame.orderBy("x").df.as[Int].collect().toList shouldBe List(1, 2, 3)
    }

    "order by fields descending" in {
      val df = Seq(3, 1, 2).toDF("x")
      df.frame.orderBy("x", descending = true).df.as[Int].collect().toList shouldBe List(3, 2, 1)
    }

    "throw when fields are empty" in {
      val df = Seq((1, "a")).toDF("x", "y")
      val ex = intercept[IllegalArgumentException] {
        df.frame.orderBy(Fields.empty)
      }
      ex.getMessage should include("orderBy requires at least one field")
    }
  }

  "Frame.orderBy (Seq[(Field, Boolean)])" should {
    "support per-field ascending/descending flags" in {
      val df = Seq((1, "b"), (1, "a"), (2, "c")).toDF("k", "v")

      val out =
        df.frame
          .orderBy(Seq(Field("k") -> false, Field("v") -> true))
          .df

      out.collect().toList shouldBe List(
        Row(1, "b"),
        Row(1, "a"),
        Row(2, "c")
      )
    }

    "throw when fields are empty" in {
      val df = Seq((1, "a")).toDF("x", "y")
      val ex = intercept[IllegalArgumentException] {
        df.frame.orderBy(Seq.empty[(Field, Boolean)])
      }
      ex.getMessage should include("orderBy requires at least one field")
    }
  }
}
