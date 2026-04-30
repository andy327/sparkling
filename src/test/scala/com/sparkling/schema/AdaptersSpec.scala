package com.sparkling.schema

import org.scalatest.matchers.should.Matchers._
import org.scalatest.wordspec.AnyWordSpec

final class AdaptersSpec extends AnyWordSpec {

  "ToField" should {
    "summon from Field and String" in {
      val f1 = ToField[Field].toField(Field("id"))
      f1 shouldBe Field("id")

      val f2 = ToField[String].toField("name")
      f2 shouldBe Field("name")
    }
  }

  "ToFields" should {
    "summon from Fields, Field, String, and Iterable[String]" in {
      ToFields[Fields].toFields(Fields("a", "b")) shouldBe Fields("a", "b")
      ToFields[Field].toFields(Field("id")) shouldBe Fields("id")
      ToFields[String].toFields("x") shouldBe Fields("x")
      ToFields[List[String]].toFields(List("m", "n")).names shouldBe Vector("m", "n")
    }

    "respect strict validation in conversions" in {
      an[IllegalArgumentException] shouldBe thrownBy(ToField[String].toField(" bad"))
      an[IllegalArgumentException] shouldBe thrownBy(ToFields[List[String]].toFields(List("ok", " bad")))
    }
  }
}
