package com.sparkling.schema

import org.scalatest.matchers.should.Matchers._
import org.scalatest.wordspec.AnyWordSpec

final class FieldsLikeSpec extends AnyWordSpec {

  "FieldsLike" should {
    "normalize Field to Fields and expose arity/contains" in {
      val fl: FieldsLike = Field("a")
      fl.arity shouldBe 1
      fl.isSingle shouldBe true
      fl.contains("a") shouldBe true
      fl.contains("b") shouldBe false
      fl.fields shouldBe Fields("a")
      fl.prefix("p_").names shouldBe Vector("p_a")
      fl.postfix("_x").names shouldBe Vector("a_x")
      fl.toString shouldBe "[a]"
    }

    "keep Fields as-is and apply transforms" in {
      val fl: FieldsLike = Fields("x", "y")
      fl.arity shouldBe 2
      fl.isSingle shouldBe false
      fl.names shouldBe Vector("x", "y")
      fl.fields shouldBe Fields("x", "y")
      fl.prefix("pre_").names shouldBe Vector("pre_x", "pre_y")
      fl.postfix("_suf").names shouldBe Vector("x_suf", "y_suf")
      fl.toString shouldBe "[x,y]"
    }
  }
}
