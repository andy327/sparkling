package com.sparkling.frame

import org.apache.spark.sql.Row
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import com.sparkling.dsl._
import com.sparkling.schema.Fields
import com.sparkling.testkit.SparkSuite

final class FrameStreamSpec extends AnyWordSpec with Matchers with SparkSuite {
  import spark.implicits._

  "Frame.streamBy" should {
    "group by keys and emit one output row per group (keys ++ out)" in {
      val input = Seq(
        ("NYC", "US", 10, "a"),
        ("NYC", "US", 20, "b"),
        ("Paris", "FR", 5, "c")
      ).toDF("city", "country", "score", "name").frame

      val result =
        input.streamBy(Fields("city", "country")) {
          _.mapGroups(Fields("score", "name") -> Fields("total")) { it: Iterator[(Int, String)] =>
            Iterator(it.map(_._1).sum)
          }
        }.df.orderBy("city", "country")

      result.columns.toSeq shouldBe Seq("city", "country", "total")
      result.collect().toList shouldBe List(
        Row("NYC", "US", 30),
        Row("Paris", "FR", 5)
      )
    }

    "use a deterministic fallback ordering when no sortBy is configured" in {
      val input = Seq(
        ("NYC", "US", 20, "b"),
        ("NYC", "US", 10, "a"),
        ("NYC", "US", 30, "c")
      ).toDF("city", "country", "score", "name").frame

      val result =
        input.streamBy(Fields("city", "country")) {
          _.mapGroups(Fields("score", "name") -> Fields("total")) { it: Iterator[(Int, String)] =>
            Iterator(it.map(_._1).next())
          }
        }.df

      result.collect().toList shouldBe List(Row("NYC", "US", 10))
    }

    "honor sortBy ascending" in {
      val input = Seq(
        ("NYC", "US", 20, "b"),
        ("NYC", "US", 10, "a"),
        ("NYC", "US", 30, "c")
      ).toDF("city", "country", "score", "name").frame

      val result =
        input.streamBy(Fields("city", "country")) {
          _.sortBy(Fields("score"))
            .mapGroups(Fields("score", "name") -> Fields("total")) { it: Iterator[(Int, String)] =>
              Iterator(it.map(_._1).next())
            }
        }.df

      result.collect().toList shouldBe List(Row("NYC", "US", 10))
    }

    "honor sortBy descending via reverse" in {
      val input = Seq(
        ("NYC", "US", 10, "a"),
        ("NYC", "US", 20, "b"),
        ("NYC", "US", 30, "c")
      ).toDF("city", "country", "score", "name").frame

      val result =
        input.streamBy(Fields("city", "country")) {
          _.sortBy(Fields("score")).reverse
            .mapGroups(Fields("score", "name") -> Fields("total")) { it: Iterator[(Int, String)] =>
              Iterator(it.map(_._1).next())
            }
        }.df

      result.collect().toList shouldBe List(Row("NYC", "US", 30))
    }

    "correctly decode inputs when sort fields differ from input fields" in {
      val input = Seq(
        ("NYC", "US", 10, "c"),
        ("NYC", "US", 20, "b"),
        ("NYC", "US", 30, "a")
      ).toDF("city", "country", "score", "name").frame

      val result =
        input.streamBy(Fields("city", "country")) {
          _.sortBy(Fields("name"))
            .mapGroups(Fields("score", "name") -> Fields("total")) { it: Iterator[(Int, String)] =>
              Iterator(it.map(_._1).sum)
            }
        }.df

      result.collect().toList shouldBe List(Row("NYC", "US", 60))
    }

    "emit null outputs through the encoder path" in {
      val input = Seq(
        ("NYC", "US", 10, "a"),
        ("Paris", "FR", 5, "b")
      ).toDF("city", "country", "score", "name").frame

      val result =
        input.streamBy(Fields("city", "country")) {
          _.mapGroups(Fields("score", "name") -> Fields("maybe_name")) { _: Iterator[(Int, String)] =>
            Iterator.single(null.asInstanceOf[String])
          }
        }.df.orderBy("city", "country")

      result.columns.toSeq shouldBe Seq("city", "country", "maybe_name")
      result.collect().toList shouldBe List(
        Row("NYC", "US", null),
        Row("Paris", "FR", null)
      )
    }

    "emit zero output rows when the iterator is empty" in {
      val input = Seq(
        ("NYC", "US", 10, "a"),
        ("Paris", "FR", 5, "b")
      ).toDF("city", "country", "score", "name").frame

      val result =
        input.streamBy(Fields("city", "country")) {
          _.mapGroups(Fields("score", "name") -> Fields("total")) { _: Iterator[(Int, String)] =>
            Iterator.empty: Iterator[Int]
          }
        }.df

      result.collect().toList shouldBe List.empty
    }

    "support mapStreamWithContext for stateful per-group processing" in {
      val input = Seq(
        ("A", 1),
        ("A", 2),
        ("A", 3),
        ("B", 10)
      ).toDF("group", "value").frame

      val result =
        input.streamBy(Fields("group")) {
          _.sortBy(Fields("value"))
            .mapStreamWithContext(Fields("value") -> Fields("running_sum"))(0) { (acc: Int, it: Iterator[Int]) =>
              it.scanLeft(acc)(_ + _).drop(1)
            }
        }.df.orderBy("group", "running_sum")

      result.columns.toSeq shouldBe Seq("group", "running_sum")
      result.collect().toList shouldBe List(
        Row("A", 1),
        Row("A", 3),
        Row("A", 6),
        Row("B", 10)
      )
    }

    "throw when keys are empty" in {
      val frame = Seq((1, "a")).toDF("x", "y").frame
      val ex = intercept[IllegalArgumentException] {
        frame.streamBy(Fields.empty)(_.mapGroups(Fields("x") -> Fields("x")) { it: Iterator[Int] =>
          it
        })
      }
      ex.getMessage should include("streamBy requires at least one key field")
    }

    "throw when no stream operation is defined" in {
      val frame = Seq((1, "a")).toDF("x", "y").frame
      val ex = intercept[IllegalArgumentException] {
        frame.streamBy(Fields("x"))(identity)
      }
      ex.getMessage should include("requires exactly one stream operation")
    }
  }

  "Frame.streamAll" should {
    "treat all rows as a single group and emit only output columns" in {
      val input = Seq(
        ("NYC", "US", 10, "a"),
        ("Paris", "FR", 5, "b")
      ).toDF("city", "country", "score", "name").frame

      val result =
        input.streamAll {
          _.mapGroups(Fields("score", "name") -> Fields("total")) { it: Iterator[(Int, String)] =>
            Iterator(it.map(_._1).sum)
          }
        }.df

      result.columns.toSeq shouldBe Seq("total")
      result.collect().toList shouldBe List(Row(15))
    }

    "apply sorting within the single group when sortBy is configured" in {
      val input = Seq(
        ("NYC", "US", 10, "a"),
        ("Paris", "FR", 5, "b"),
        ("LA", "US", 20, "c")
      ).toDF("city", "country", "score", "name").frame

      val result =
        input.streamAll {
          _.sortBy(Fields("score")).reverse
            .mapGroups(Fields("score", "name") -> Fields("total")) { it: Iterator[(Int, String)] =>
              Iterator(it.map(_._1).next())
            }
        }.df

      result.collect().toList shouldBe List(Row(20))
    }

    "throw when no stream operation is defined" in {
      val frame = Seq((1, "a")).toDF("x", "y").frame
      val ex = intercept[IllegalArgumentException] {
        frame.streamAll(identity)
      }
      ex.getMessage should include("requires exactly one stream operation")
    }
  }

  "GroupedStream guards" should {
    "throw when sortBy fields are empty" in {
      val frame = Seq((1, "a")).toDF("x", "y").frame
      val ex = intercept[IllegalArgumentException] {
        frame.streamBy(Fields("x"))(_.sortBy(Fields.empty).mapGroups(Fields("y") -> Fields("y")) {
          it: Iterator[String] => it
        })
      }
      ex.getMessage should include("sortBy requires at least one field")
    }

    "throw when sortBy is called twice" in {
      val frame = Seq((1, "a")).toDF("x", "y").frame
      val ex = intercept[IllegalArgumentException] {
        frame.streamBy(Fields("x"))(
          _.sortBy(Fields("y"))
            .sortBy(Fields("y"))
            .mapGroups(Fields("y") -> Fields("y")) { it: Iterator[String] => it }
        )
      }
      ex.getMessage should include("sort already configured")
    }

    "throw when reverse is called without sortBy" in {
      val frame = Seq((1, "a")).toDF("x", "y").frame
      val ex = intercept[IllegalArgumentException] {
        frame.streamBy(Fields("x"))(
          _.reverse.mapGroups(Fields("y") -> Fields("y")) { it: Iterator[String] => it }
        )
      }
      ex.getMessage should include("reverse: first set fields to sort on")
    }

    "throw when mapGroups input fields are empty" in {
      val frame = Seq((1, "a")).toDF("x", "y").frame
      val ex = intercept[IllegalArgumentException] {
        frame.streamBy(Fields("x"))(_.mapGroups(Fields.empty -> Fields("y")) { it: Iterator[String] =>
          it
        })
      }
      ex.getMessage should include("mapGroups input fields must be non-empty")
    }

    "throw when mapGroups output fields are empty" in {
      val frame = Seq((1, "a")).toDF("x", "y").frame
      val ex = intercept[IllegalArgumentException] {
        frame.streamBy(Fields("x"))(_.mapGroups(Fields("y") -> Fields.empty) { it: Iterator[String] =>
          it
        })
      }
      ex.getMessage should include("mapGroups output fields must be non-empty")
    }

    "throw when mapStreamWithContext input fields are empty" in {
      val frame = Seq((1, "a")).toDF("x", "y").frame
      val ex = intercept[IllegalArgumentException] {
        frame.streamBy(Fields("x"))(
          _.mapStreamWithContext(Fields.empty -> Fields("y"))(())((_: Unit, it: Iterator[String]) => it)
        )
      }
      ex.getMessage should include("mapStreamWithContext input fields must be non-empty")
    }

    "throw when mapStreamWithContext output fields are empty" in {
      val frame = Seq((1, "a")).toDF("x", "y").frame
      val ex = intercept[IllegalArgumentException] {
        frame.streamBy(Fields("x"))(
          _.mapStreamWithContext(Fields("y") -> Fields.empty)(())((_: Unit, it: Iterator[String]) => it)
        )
      }
      ex.getMessage should include("mapStreamWithContext output fields must be non-empty")
    }
  }
}
