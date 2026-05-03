package com.sparkling.algebird

import org.scalatest.matchers.should.Matchers._
import org.scalatest.wordspec.AnyWordSpec

final class BufferSerDeSpec extends AnyWordSpec {

  private def roundTrip[B](value: B)(implicit serde: BufferSerDe[B]): B =
    serde.fromBytes(serde.toBytes(value))

  "BufferSerDe.longSerDe" should {
    "round-trip a Long" in {
      roundTrip(Long.MaxValue) shouldBe Long.MaxValue
      roundTrip(0L) shouldBe 0L
      roundTrip(-42L) shouldBe -42L
    }
  }

  "BufferSerDe.intSerDe" should {
    "round-trip an Int" in {
      roundTrip(Int.MaxValue) shouldBe Int.MaxValue
      roundTrip(0) shouldBe 0
      roundTrip(-7) shouldBe -7
    }
  }

  "BufferSerDe.doubleSerDe" should {
    "round-trip a Double" in {
      roundTrip(3.14159) shouldBe 3.14159
      roundTrip(Double.NaN).isNaN shouldBe true
      roundTrip(Double.NegativeInfinity) shouldBe Double.NegativeInfinity
    }
  }

  "BufferSerDe.floatSerDe" should {
    "round-trip a Float" in {
      roundTrip(2.718f) shouldBe 2.718f
      roundTrip(0.0f) shouldBe 0.0f
    }
  }

  "BufferSerDe.booleanSerDe" should {
    "round-trip true and false" in {
      roundTrip(true) shouldBe true
      roundTrip(false) shouldBe false
    }
  }

  "BufferSerDe.stringSerDe" should {
    "round-trip a String" in {
      roundTrip("hello") shouldBe "hello"
      roundTrip("") shouldBe ""
      roundTrip("unicode: é中文") shouldBe "unicode: é中文"
    }
  }

  "BufferSerDe.optionSerDe" should {
    "round-trip None" in {
      roundTrip[Option[Long]](None) shouldBe None
    }

    "round-trip Some(value)" in {
      roundTrip(Some(42L)) shouldBe Some(42L)
    }

    "round-trip nested Option" in {
      roundTrip[Option[Option[Long]]](Some(Some(99L))) shouldBe Some(Some(99L))
      roundTrip[Option[Option[Long]]](Some(None)) shouldBe Some(None)
      roundTrip[Option[Option[Long]]](None) shouldBe None
    }
  }
}
