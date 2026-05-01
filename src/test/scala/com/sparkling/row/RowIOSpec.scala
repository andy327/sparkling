package com.sparkling.row

import org.scalatest.matchers.should.Matchers._
import org.scalatest.wordspec.AnyWordSpec

import com.sparkling.row.codec.FieldPresence
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

  "FieldPresence" should {
    "consider String present if and only if non-null and non-blank" in {
      implicitly[FieldPresence[String]].isPresent("x") shouldBe true
      implicitly[FieldPresence[String]].isPresent(null.asInstanceOf[String]) shouldBe false
      implicitly[FieldPresence[String]].isPresent("   ") shouldBe false
    }

    "consider Int and Long always present" in {
      implicitly[FieldPresence[Int]].isPresent(0) shouldBe true
      implicitly[FieldPresence[Long]].isPresent(0L) shouldBe true
    }

    "consider Double absent when NaN" in {
      implicitly[FieldPresence[Double]].isPresent(1.0) shouldBe true
      implicitly[FieldPresence[Double]].isPresent(Double.NaN) shouldBe false
    }

    "compose for tuples" in {
      val c2 = implicitly[FieldPresence[(String, Int)]]
      c2.isPresent(("ok", 1)) shouldBe true
      c2.isPresent((null.asInstanceOf[String], 1)) shouldBe false

      val c3 = implicitly[FieldPresence[(String, Int, Double)]]
      c3.isPresent(("ok", 1, 2.0)) shouldBe true
      c3.isPresent(("ok", 1, Double.NaN)) shouldBe false
    }

    "build a predicate-backed FieldPresence instance" in {
      val even = FieldPresence[Int](_ % 2 == 0)
      even.isPresent(2) shouldBe true
      even.isPresent(3) shouldBe false
    }
  }
}
