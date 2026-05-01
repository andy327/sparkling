package com.sparkling.evidence

import org.scalatest.matchers.should.Matchers._
import org.scalatest.wordspec.AnyWordSpec

final class ExplodeEvidenceSpec extends AnyWordSpec {

  "ExplodeEvidence" should {
    "provide evidence for Seq and preserve elements" in {
      val ev = implicitly[ExplodeEvidence.Aux[Seq[Int], Int]]
      ev.toSeq(Seq(1, 2, 3)) shouldBe Seq(1, 2, 3)
    }

    "treat null Seq as empty" in {
      val ev = implicitly[ExplodeEvidence.Aux[Seq[Int], Int]]
      ev.toSeq(null.asInstanceOf[Seq[Int]]) shouldBe Seq.empty
    }

    "compile for multiple Seq subtypes" in {
      implicitly[ExplodeEvidence.Aux[List[Int], Int]]
      implicitly[ExplodeEvidence.Aux[Vector[Int], Int]]
    }

    "provide evidence for Array and preserve elements (as a Seq)" in {
      val ev = implicitly[ExplodeEvidence.Aux[Array[Int], Int]]
      ev.toSeq(Array(4, 5, 6)) shouldBe Seq(4, 5, 6)
    }

    "treat null Array as empty" in {
      val ev = implicitly[ExplodeEvidence.Aux[Array[Int], Int]]
      ev.toSeq(null.asInstanceOf[Array[Int]]) shouldBe Seq.empty
    }

    "provide evidence for Option and preserve elements" in {
      val ev = implicitly[ExplodeEvidence.Aux[Option[Int], Int]]
      ev.toSeq(Some(7)) shouldBe Seq(7)
      ev.toSeq(None) shouldBe Seq.empty
    }

    "treat null Option as empty" in {
      val ev = implicitly[ExplodeEvidence.Aux[Option[Int], Int]]
      ev.toSeq(null.asInstanceOf[Option[Int]]) shouldBe Seq.empty
    }
  }
}
