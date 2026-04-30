package com.sparkling.schema

import org.scalatest.matchers.should.Matchers._
import org.scalatest.wordspec.AnyWordSpec

final class FieldSpec extends AnyWordSpec {

  "Field" should {
    "preserve exact name and convert to Fields" in {
      val f = Field("name")
      f.name shouldBe "name"
      f.names shouldBe Vector("name")
      f.toFields.names shouldBe Vector("name")
      f.fields.names shouldBe Vector("name")
      f.arity shouldBe 1
      f.isSingle shouldBe true
      f.contains("name") shouldBe true
      f.contains("other") shouldBe false
      f.toString shouldBe "[name]"
    }

    "implement equals/hashCode correctly" in {
      val a1 = Field("a")
      val a2 = Field("a")
      val b = Field("b")

      a1 shouldBe a2
      (a1 == b) shouldBe false
      (a1.equals("not-a-field")) shouldBe false

      a1.hashCode shouldBe "a".hashCode
      a2.hashCode shouldBe a1.hashCode
      (a1.hashCode == b.hashCode) shouldBe false
    }

    "reject null, empty, or whitespace-padded names" in {
      an[IllegalArgumentException] shouldBe thrownBy(Field(null))
      an[IllegalArgumentException] shouldBe thrownBy(Field(""))
      an[IllegalArgumentException] shouldBe thrownBy(Field(" name"))
      an[IllegalArgumentException] shouldBe thrownBy(Field("name "))
      an[IllegalArgumentException] shouldBe thrownBy(Field(" name "))
    }

    "generate temporary names via tmp with the default prefix" in {
      val f1 = Field.tmp()
      val f2 = Field.tmp()
      f1.name should startWith("tmp-")
      f2.name should startWith("tmp-")
      f1.name should not be f2.name
      f1.name.stripPrefix("tmp-").length shouldBe 16
      (f1.name.stripPrefix("tmp-") should fullyMatch).regex("^[a-f0-9]+$")
    }

    "generate temporary names via tmp with a custom prefix" in {
      val f = Field.tmp("stage-")
      f.name should startWith("stage-")
      f.name.stripPrefix("stage-").length shouldBe 16

      an[IllegalArgumentException] shouldBe thrownBy(Field.tmp(" bad"))
      an[IllegalArgumentException] shouldBe thrownBy(Field.tmp("bad "))
      an[IllegalArgumentException] shouldBe thrownBy(Field.tmp(""))
    }
  }
}
