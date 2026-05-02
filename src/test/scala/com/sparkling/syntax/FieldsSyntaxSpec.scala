package com.sparkling.syntax

import org.scalatest.matchers.should.Matchers._
import org.scalatest.wordspec.AnyWordSpec

import com.sparkling.schema.{Field, Fields}

final class FieldsSyntaxSpec extends AnyWordSpec {
  import FieldsSyntax._

  "FieldsSyntax.stringToField" should {
    "convert a String to a Field" in {
      val f: Field = "x"
      f.name shouldBe "x"
    }
  }

  "FieldsSyntax.stringToFields" should {
    "convert a String to a single-element Fields" in {
      val fs: Fields = "x"
      fs.names shouldBe Seq("x")
    }
  }

  "FieldsSyntax.seqStringToFields" should {
    "convert a Seq[String] to Fields" in {
      val fs: Fields = Seq("a", "b", "c")
      fs.names shouldBe Seq("a", "b", "c")
    }
  }

  "FieldsSyntax tuple -> Fields conversions" should {
    "convert a 2-tuple of Strings to Fields" in {
      val fs: Fields = ("a", "b")
      fs.names shouldBe Seq("a", "b")
    }

    "convert a 3-tuple of Strings to Fields" in {
      val fs: Fields = ("a", "b", "c")
      fs.names shouldBe Seq("a", "b", "c")
    }

    "convert a 22-tuple of Strings to Fields" in {
      val fs: Fields =
        (
          "f1",
          "f2",
          "f3",
          "f4",
          "f5",
          "f6",
          "f7",
          "f8",
          "f9",
          "f10",
          "f11",
          "f12",
          "f13",
          "f14",
          "f15",
          "f16",
          "f17",
          "f18",
          "f19",
          "f20",
          "f21",
          "f22"
        )
      fs.size shouldBe 22
      fs.names.head shouldBe "f1"
      fs.names.last shouldBe "f22"
    }
  }

  "FieldsSyntax.ToFields" should {
    "resolve from String" in {
      val ev = implicitly[ToFields[String]]
      ev.toFields("x").names shouldBe Seq("x")
    }

    "resolve from Field" in {
      val ev = implicitly[ToFields[Field]]
      ev.toFields(Field("x")).names shouldBe Seq("x")
    }

    "resolve from Fields" in {
      val fs = Fields("x", "y")
      val ev = implicitly[ToFields[Fields]]
      (ev.toFields(fs) eq fs) shouldBe true
    }

    "resolve from a 2-tuple of Strings" in {
      val ev = implicitly[ToFields[(String, String)]]
      ev.toFields(("a", "b")).names shouldBe Seq("a", "b")
    }

    "resolve from a 3-tuple of Strings" in {
      val ev = implicitly[ToFields[(String, String, String)]]
      ev.toFields(("a", "b", "c")).names shouldBe Seq("a", "b", "c")
    }
  }

  "FieldsSyntax.ToField" should {
    "resolve from String" in {
      val ev = implicitly[ToField[String]]
      ev.toField("x").name shouldBe "x"
    }

    "resolve from Field" in {
      val f = Field("x")
      val ev = implicitly[ToField[Field]]
      (ev.toField(f) eq f) shouldBe true
    }

    "resolve from a single-element Fields" in {
      val ev = implicitly[ToField[Fields]]
      ev.toField(Fields("x")).name shouldBe "x"
    }

    "throw when resolving ToField from a multi-element Fields" in {
      val ev = implicitly[ToField[Fields]]
      val ex = intercept[IllegalArgumentException] {
        ev.toField(Fields("x", "y"))
      }
      ex.getMessage should include("expected exactly one field")
    }
  }

  "FieldsSyntax.fieldsToFieldsMapping" should {
    "convert (String, String) to (Fields, Fields)" in {
      val m: (Fields, Fields) = "a" -> "b"
      m._1.names shouldBe Seq("a")
      m._2.names shouldBe Seq("b")
    }

    "convert ((String,String), (String,String)) to (Fields, Fields)" in {
      val m: (Fields, Fields) = ("a", "b") -> ("x", "y")
      m._1.names shouldBe Seq("a", "b")
      m._2.names shouldBe Seq("x", "y")
    }

    "convert (Fields, Fields) to (Fields, Fields) with identity" in {
      val left = Fields("a", "b")
      val right = Fields("x", "y")
      val m: (Fields, Fields) = left -> right
      (m._1 eq left) shouldBe true
      (m._2 eq right) shouldBe true
    }
  }

  "FieldsSyntax.fieldsToFieldMapping" should {
    "convert ((String,String), String) to (Fields, Field)" in {
      val m: (Fields, Field) = ("a", "b") -> "out"
      m._1.names shouldBe Seq("a", "b")
      m._2.name shouldBe "out"
    }

    "convert (String, String) to (Fields, Field)" in {
      val m: (Fields, Field) = "a" -> Field("out")
      m._1.names shouldBe Seq("a")
      m._2.name shouldBe "out"
    }
  }

  "FieldsSyntax.fieldToFieldsMapping" should {
    "convert (String, (String,String)) to (Field, Fields)" in {
      val m: (Field, Fields) = "in" -> ("a", "b")
      m._1.name shouldBe "in"
      m._2.names shouldBe Seq("a", "b")
    }

    "convert (String, String) to (Field, Fields)" in {
      val m: (Field, Fields) = Field("in") -> "out"
      m._1.name shouldBe "in"
      m._2.names shouldBe Seq("out")
    }
  }
}
