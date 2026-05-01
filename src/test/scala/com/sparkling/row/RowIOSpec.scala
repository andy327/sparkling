package com.sparkling.row

import org.scalatest.matchers.should.Matchers._
import org.scalatest.wordspec.AnyWordSpec

import com.sparkling.row.codec.Completeness
import com.sparkling.schema.Fields

final class RowIOSpec extends AnyWordSpec {

  "RowUtil" should {
    "report presence correctly with allPresent" in {
      val schema = Fields("a", "b", "c")
      val row = Row.fromArray(schema, Array("x", 123, 3.14))
      RowUtil.allPresent(row, Fields("a")) shouldBe true
      RowUtil.allPresent(row, Fields("a", "b")) shouldBe true
      RowUtil.allPresent(row, Fields("a", "z")) shouldBe false
      RowUtil.allPresent(row, Fields("c")) shouldBe true

      val withNull = Row.fromArray(schema, Array("x", null, 3.14))
      RowUtil.allPresent(withNull, Fields("b")) shouldBe false
      RowUtil.allPresent(withNull, Fields("a", "b")) shouldBe false
      RowUtil.allPresent(withNull, Fields("a", "c")) shouldBe true
    }

    "return raw values in order with raw()" in {
      val schema = Fields("a", "b", "c")
      val row = Row.fromArray(schema, Array("x", null, 3.14))
      RowUtil.raw(row, Fields("a", "b", "c")) shouldBe Vector(Some("x"), None, Some(3.14))
    }
  }

  "Completeness" should {
    "consider String complete if and only if non-null and non-blank" in {
      implicitly[Completeness[String]].isComplete("x") shouldBe true
      implicitly[Completeness[String]].isComplete(null.asInstanceOf[String]) shouldBe false
      implicitly[Completeness[String]].isComplete("   ") shouldBe false
    }

    "consider Int and Long always complete" in {
      implicitly[Completeness[Int]].isComplete(0) shouldBe true
      implicitly[Completeness[Long]].isComplete(0L) shouldBe true
    }

    "consider Double incomplete when NaN" in {
      implicitly[Completeness[Double]].isComplete(1.0) shouldBe true
      implicitly[Completeness[Double]].isComplete(Double.NaN) shouldBe false
    }

    "compose for tuples" in {
      val c2 = implicitly[Completeness[(String, Int)]]
      c2.isComplete(("ok", 1)) shouldBe true
      c2.isComplete((null.asInstanceOf[String], 1)) shouldBe false

      val c3 = implicitly[Completeness[(String, Int, Double)]]
      c3.isComplete(("ok", 1, 2.0)) shouldBe true
      c3.isComplete(("ok", 1, Double.NaN)) shouldBe false
    }

    "build a predicate-backed Completeness instance" in {
      val even = Completeness[Int](_ % 2 == 0)
      even.isComplete(2) shouldBe true
      even.isComplete(3) shouldBe false
    }
  }
}
