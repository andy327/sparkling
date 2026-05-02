package com.sparkling.frame

import org.apache.spark.sql.Row
import org.scalatest.matchers.should.Matchers._
import org.scalatest.wordspec.AnyWordSpec

import com.sparkling.schema.Fields
import com.sparkling.syntax.FieldsSyntax._
import com.sparkling.syntax.FrameSyntax._
import com.sparkling.testkit.SparkSuite

final class FrameRelationalSpec extends AnyWordSpec with SparkSuite {
  import spark.implicits._

  "Frame.broadcast" should {
    "return a Frame with the same data" in {
      val df = Seq((1, "a"), (2, "b")).toDF("x", "y")
      val out = df.frame.broadcast().df

      out.orderBy("x").collect().toList shouldBe List(Row(1, "a"), Row(2, "b"))
    }
  }

  "Frame.join (Fields, Fields)" should {
    "inner join on asymmetric key names" in {
      val left = Seq((1, "a"), (2, "b"), (3, "c")).toDF("id", "v")
      val right = Seq((1, "x"), (2, "y")).toDF("rid", "rv")
      val out = left.frame
        .join(Fields("id") -> Fields("rid"), right.frame, JoinType.Inner)
        .orderBy("id")
        .df

      out.select("id", "v", "rv").collect().toList shouldBe
        List(Row(1, "a", "x"), Row(2, "b", "y"))
    }

    "left join produces nulls for unmatched right rows" in {
      val left = Seq((1, "a"), (2, "b")).toDF("id", "v")
      val right = Seq((1, "x")).toDF("rid", "rv")
      val out = left.frame
        .join(Fields("id") -> Fields("rid"), right.frame, JoinType.Left)
        .orderBy("id")
        .df

      out.select("id", "v", "rv").collect().toList shouldBe
        List(Row(1, "a", "x"), Row(2, "b", null))
    }

    "right join produces nulls for unmatched left rows" in {
      val left = Seq((1, "a")).toDF("id", "v")
      val right = Seq((1, "x"), (2, "y")).toDF("rid", "rv")
      val out = left.frame
        .join(Fields("id") -> Fields("rid"), right.frame, JoinType.Right)
        .orderBy("rid")
        .df

      out.select("id", "v", "rv").collect().toList shouldBe
        List(Row(1, "a", "x"), Row(null, null, "y"))
    }

    "outer join keeps all rows from both sides" in {
      val left = Seq((1, "a")).toDF("id", "v")
      val right = Seq((2, "y")).toDF("rid", "rv")
      val out = left.frame
        .join(Fields("id") -> Fields("rid"), right.frame, JoinType.Outer)
        .df

      out.count() shouldBe 2L
    }

    "left semi join keeps only matching left rows without right columns" in {
      val left = Seq((1, "a"), (2, "b"), (3, "c")).toDF("id", "v")
      val right = Seq((1, "x"), (3, "z")).toDF("rid", "rv")
      val out = left.frame
        .join(Fields("id") -> Fields("rid"), right.frame, JoinType.LeftSemi)
        .orderBy("id")
        .df

      out.columns.toSeq shouldBe Seq("id", "v")
      out.collect().toList shouldBe List(Row(1, "a"), Row(3, "c"))
    }

    "left anti join keeps only non-matching left rows" in {
      val left = Seq((1, "a"), (2, "b"), (3, "c")).toDF("id", "v")
      val right = Seq((1, "x"), (3, "z")).toDF("rid", "rv")
      val out = left.frame
        .join(Fields("id") -> Fields("rid"), right.frame, JoinType.LeftAnti)
        .df

      out.collect().toList shouldBe List(Row(2, "b"))
    }

    "join on multiple key columns (compound key)" in {
      val left = Seq((1, "a", "x"), (1, "b", "y"), (2, "a", "z")).toDF("id", "code", "v")
      val right = Seq((1, "a", "p"), (2, "a", "q")).toDF("rid", "rcode", "rv")
      val out = left.frame
        .join(Fields("id", "code") -> Fields("rid", "rcode"), right.frame, JoinType.Inner)
        .orderBy(Fields("id", "code"))
        .df

      out.select("id", "code", "v", "rv").collect().toList shouldBe
        List(Row(1, "a", "x", "p"), Row(2, "a", "z", "q"))
    }

    "throw when key arities differ" in {
      val left = Seq((1, "a")).toDF("id", "v")
      val right = Seq((1, "x")).toDF("rid", "rv")
      val ex = intercept[IllegalArgumentException] {
        left.frame.join(Fields("id") -> Fields.empty, right.frame, JoinType.Inner)
      }
      ex.getMessage should include("join requires same arity")
    }

    "throw when both key sets are empty" in {
      val left = Seq((1, "a")).toDF("id", "v")
      val right = Seq((1, "x")).toDF("rid", "rv")
      val ex = intercept[IllegalArgumentException] {
        left.frame.join(Fields.empty -> Fields.empty, right.frame, JoinType.Inner)
      }
      ex.getMessage should include("join requires at least one key field")
    }
  }

  "Frame.join (Fields)" should {
    "inner join on shared key name using default" in {
      val left = Seq((1, "a"), (2, "b")).toDF("id", "v")
      val right = Seq((1, "x"), (2, "y")).toDF("id", "rv")
      val out = left.frame
        .join(Fields("id"), right.frame)
        .orderBy("id")
        .df

      out.select("id", "v", "rv").collect().toList shouldBe
        List(Row(1, "a", "x"), Row(2, "b", "y"))
    }

    "respect the how parameter" in {
      val left = Seq((1, "a"), (2, "b")).toDF("id", "v")
      val right = Seq((1, "x")).toDF("id", "rv")
      val out = left.frame
        .join(Fields("id"), right.frame, JoinType.LeftSemi)
        .df

      out.collect().toList shouldBe List(Row(1, "a"))
    }
  }

  "Frame.++" should {
    "stack rows from two frames with the same schema" in {
      val a = Seq((1, "a"), (2, "b")).toDF("x", "y")
      val b = Seq((3, "c")).toDF("x", "y")
      val out = (a.frame ++ b.frame).orderBy("x").df

      out.collect().toList shouldBe List(Row(1, "a"), Row(2, "b"), Row(3, "c"))
    }

    "pad missing columns with null when schemas differ" in {
      val a = Seq((1, "a")).toDF("x", "y")
      val b = Seq((2, "b", true)).toDF("x", "y", "z")
      val out = (a.frame ++ b.frame).orderBy("x").df

      out.columns.toSeq shouldBe Seq("x", "y", "z")
      out.collect().toList shouldBe List(Row(1, "a", null), Row(2, "b", true))
    }
  }

  "Frame.merge" should {
    "return the single frame unchanged when given one input" in {
      val a = Seq((1, "a")).toDF("x", "y")
      Frame.merge(Seq(a.frame)).df.collect().toList shouldBe List(Row(1, "a"))
    }

    "union frames with identical schemas" in {
      val a = Seq((1, "a")).toDF("x", "y")
      val b = Seq((2, "b")).toDF("x", "y")
      val out = Frame.merge(Seq(a.frame, b.frame)).orderBy("x").df

      out.collect().toList shouldBe List(Row(1, "a"), Row(2, "b"))
    }

    "pad missing columns with null when keepAll = true (default)" in {
      val a = Seq((1, "a")).toDF("x", "y")
      val b = Seq((2, "b", true)).toDF("x", "y", "z")
      val out = Frame.merge(Seq(a.frame, b.frame)).orderBy("x").df

      out.columns.toSeq shouldBe Seq("x", "y", "z")
      out.collect().toList shouldBe List(Row(1, "a", null), Row(2, "b", true))
    }

    "project to common columns only when keepAll = false" in {
      val a = Seq((1, "a")).toDF("x", "y")
      val b = Seq((2, "b", true)).toDF("x", "y", "z")
      val out = Frame.merge(Seq(a.frame, b.frame), keepAll = false).orderBy("x").df

      out.columns.toSeq shouldBe Seq("x", "y")
      out.collect().toList shouldBe List(Row(1, "a"), Row(2, "b"))
    }

    "throw when given no frames" in {
      val ex = intercept[IllegalArgumentException] {
        Frame.merge(Seq.empty)
      }
      ex.getMessage should include("merge requires at least one input")
    }

    "throw when frames share no common columns" in {
      val a = Seq(1).toDF("x")
      val b = Seq("a").toDF("y")
      val ex = intercept[IllegalArgumentException] {
        Frame.merge(Seq(a.frame, b.frame))
      }
      ex.getMessage should include("merge requires at least one column in common")
    }
  }
}
