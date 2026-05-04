package com.sparkling.frame

import org.apache.spark.sql.Row
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import com.sparkling.dsl._
import com.sparkling.schema.Fields
import com.sparkling.testkit.SparkSuite

final class FrameWindowSpec extends AnyWordSpec with Matchers with SparkSuite {
  import spark.implicits._

  // name, dept, salary, hire_order
  private def employees = Seq(
    ("alice", "eng", 100, 1),
    ("bob", "eng", 200, 2),
    ("carol", "eng", 200, 3),
    ("dave", "sales", 300, 1),
    ("eve", "sales", 150, 2)
  ).toDF("name", "dept", "salary", "hire_order").frame

  "Frame.windowBy" should {

    "assign rank within partition" in {
      val result = employees
        .windowBy("dept")(_.orderBy("salary").rank("r"))
        .orderBy(("dept", "name"))
        .df.select("name", "r")

      result.collect().toList shouldBe List(
        Row("alice", 1),
        Row("bob", 2),
        Row("carol", 2),
        Row("dave", 2),
        Row("eve", 1)
      )
    }

    "assign dense_rank within partition (no gaps for ties)" in {
      val result = employees
        .windowBy("dept")(_.orderBy("salary").denseRank("r"))
        .orderBy(("dept", "name"))
        .df.select("name", "r")

      result.collect().toList shouldBe List(
        Row("alice", 1),
        Row("bob", 2),
        Row("carol", 2),
        Row("dave", 2),
        Row("eve", 1)
      )
    }

    "assign unique row_number within partition" in {
      // secondary sort on name to break salary ties deterministically
      val result = employees
        .windowBy("dept") {
          _.orderBy(Seq("salary" -> false, "name" -> false)).rowNumber("rn")
        }
        .orderBy(("dept", "name"))
        .df.select("name", "rn")

      result.collect().toList shouldBe List(
        Row("alice", 1),
        Row("bob", 2),
        Row("carol", 3),
        Row("dave", 2),
        Row("eve", 1)
      )
    }

    "assign ntile buckets within partition" in {
      val result = employees
        .windowBy("dept")(_.orderBy("salary").ntile(2, "bucket"))
        .orderBy(("dept", "name"))
        .df.select("name", "bucket")

      // eng (3 rows, 2 buckets): alice→1, bob→1, carol→2
      // sales (2 rows, 2 buckets): eve→1, dave→2
      result.collect().toList shouldBe List(
        Row("alice", 1),
        Row("bob", 1),
        Row("carol", 2),
        Row("dave", 2),
        Row("eve", 1)
      )
    }

    "compute lag within partition (null at boundary)" in {
      val result = employees
        .windowBy("dept") {
          _.orderBy("hire_order").lag("salary" -> "prev_salary")
        }
        .orderBy(("dept", "hire_order"))
        .df.select("name", "prev_salary")

      result.collect().toList shouldBe List(
        Row("alice", null),
        Row("bob", 100),
        Row("carol", 200),
        Row("dave", null),
        Row("eve", 300)
      )
    }

    "compute lead within partition (null at boundary)" in {
      val result = employees
        .windowBy("dept") {
          _.orderBy("hire_order").lead("salary" -> "next_salary")
        }
        .orderBy(("dept", "hire_order"))
        .df.select("name", "next_salary")

      result.collect().toList shouldBe List(
        Row("alice", 200),
        Row("bob", 200),
        Row("carol", null),
        Row("dave", 150),
        Row("eve", null)
      )
    }

    "compute running sum within partition (default bounds)" in {
      val result = employees
        .windowBy("dept") {
          _.orderBy("hire_order").sum("salary" -> "running_total")
        }
        .orderBy(("dept", "hire_order"))
        .df.select("name", "running_total")

      result.collect().toList shouldBe List(
        Row("alice", 100L),
        Row("bob", 300L),
        Row("carol", 500L),
        Row("dave", 300L),
        Row("eve", 450L)
      )
    }

    "compute rolling sum with custom rowsBetween bounds" in {
      val result = employees
        .windowBy("dept") {
          _.orderBy("hire_order")
            .sum("salary" -> "rolling_2", WindowBounds.rowsBetween(-1, 0))
        }
        .orderBy(("dept", "hire_order"))
        .df.select("name", "rolling_2")

      // eng: alice=100, bob=100+200=300, carol=200+200=400
      // sales: dave=300, eve=300+150=450
      result.collect().toList shouldBe List(
        Row("alice", 100L),
        Row("bob", 300L),
        Row("carol", 400L),
        Row("dave", 300L),
        Row("eve", 450L)
      )
    }

    "compute running avg within partition" in {
      val result = employees
        .windowBy("dept") {
          _.orderBy("hire_order").avg("salary" -> "running_avg")
        }
        .orderBy(("dept", "hire_order"))
        .df.select("name", "running_avg")

      result.collect().toList shouldBe List(
        Row("alice", 100.0),
        Row("bob", 150.0),
        Row("carol", 500.0 / 3),
        Row("dave", 300.0),
        Row("eve", 225.0)
      )
    }

    "compute running min and max within partition" in {
      val result = employees
        .windowBy("dept") {
          _.orderBy("hire_order")
            .min("salary" -> "running_min")
            .max("salary" -> "running_max")
        }
        .orderBy(("dept", "hire_order"))
        .df.select("name", "running_min", "running_max")

      result.collect().toList shouldBe List(
        Row("alice", 100, 100),
        Row("bob", 100, 200),
        Row("carol", 100, 200),
        Row("dave", 300, 300),
        Row("eve", 150, 300)
      )
    }

    "apply multiple operations in a single windowBy block" in {
      val result = employees
        .windowBy("dept") {
          _.orderBy("salary")
            .rank("r")
            .lag("salary" -> "prev_salary")
        }
        .orderBy(("dept", "hire_order"))
        .df.select("name", "r", "prev_salary")

      result.collect().toList shouldBe List(
        Row("alice", 1, null),
        Row("bob", 2, 100),
        Row("carol", 2, 200),
        Row("dave", 2, 150),
        Row("eve", 1, null)
      )
    }

    "honor orderBy descending" in {
      val result = employees
        .windowBy("dept") {
          _.orderBy("salary", descending = true).rank("r")
        }
        .orderBy(("dept", "name"))
        .df.select("name", "r")

      // eng desc: carol(200)=1, bob(200)=1, alice(100)=3
      // sales desc: dave(300)=1, eve(150)=2
      result.collect().toList shouldBe List(
        Row("alice", 3),
        Row("bob", 1),
        Row("carol", 1),
        Row("dave", 1),
        Row("eve", 2)
      )
    }

    "compute percent_rank within partition" in {
      val result = employees
        .windowBy("dept")(_.orderBy("salary").percentRank("pr"))
        .orderBy(("dept", "hire_order"))
        .df.select("name", "pr")

      // eng (3 rows): alice=(1-1)/(3-1)=0.0, bob=(2-1)/(3-1)=0.5, carol=(2-1)/(3-1)=0.5
      // sales (2 rows): dave=(2-1)/(2-1)=1.0, eve=(1-1)/(2-1)=0.0
      result.collect().toList shouldBe List(
        Row("alice", 0.0),
        Row("bob", 0.5),
        Row("carol", 0.5),
        Row("dave", 1.0),
        Row("eve", 0.0)
      )
    }

    "compute running count within partition" in {
      val result = employees
        .windowBy("dept")(_.orderBy("hire_order").count("salary" -> "running_count"))
        .orderBy(("dept", "hire_order"))
        .df.select("name", "running_count")

      result.collect().toList shouldBe List(
        Row("alice", 1L),
        Row("bob", 2L),
        Row("carol", 3L),
        Row("dave", 1L),
        Row("eve", 2L)
      )
    }

    "compute aggregate with rangeBetween bounds" in {
      val result = employees
        .windowBy("dept") {
          _.orderBy("salary")
            .sum(
              "salary" -> "partition_total",
              WindowBounds.rangeBetween(
                WindowBounds.unboundedPreceding,
                WindowBounds.unboundedFollowing
              )
            )
        }
        .orderBy(("dept", "hire_order"))
        .df.select("name", "partition_total")

      // eng total: 100+200+200=500; sales total: 300+150=450
      result.collect().toList shouldBe List(
        Row("alice", 500L),
        Row("bob", 500L),
        Row("carol", 500L),
        Row("dave", 450L),
        Row("eve", 450L)
      )
    }

    "throw when orderBy(Fields) is called with empty fields" in
      intercept[IllegalArgumentException] {
        employees.windowBy("dept")(_.orderBy(Fields.empty).rank("r"))
      }

    "throw when orderBy(Seq) is called with empty seq" in
      intercept[IllegalArgumentException] {
        employees.windowBy("dept")(_.orderBy(Seq.empty[(String, Boolean)]).rank("r"))
      }

    "throw when no operations are defined" in
      intercept[IllegalArgumentException] {
        employees.windowBy("dept")(identity).df.collect()
      }

    "throw when keys are empty" in
      intercept[IllegalArgumentException] {
        employees.windowBy(Fields.empty)(_.orderBy("salary").rank("r"))
      }
  }

  "Frame.windowAll" should {

    "assign row_number across all rows" in {
      val result = employees
        .windowAll {
          _.orderBy(Seq("salary" -> false, "name" -> false)).rowNumber("global_rank")
        }
        .orderBy(("salary", "name"))
        .df.select("name", "global_rank")

      result.collect().toList shouldBe List(
        Row("alice", 1),
        Row("eve", 2),
        Row("bob", 3),
        Row("carol", 4),
        Row("dave", 5)
      )
    }

    "throw when no operations are defined" in
      intercept[IllegalArgumentException] {
        employees.windowAll(identity).df.collect()
      }
  }
}
