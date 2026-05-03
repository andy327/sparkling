package com.sparkling.algebird

import com.twitter.algebird.{Aggregator => AlbAggregator, Monoid, MonoidAggregator}
import org.scalatest.matchers.should.Matchers._
import org.scalatest.wordspec.AnyWordSpec

final class AlgebirdPackageSpec extends AnyWordSpec {

  private val sumStrLen: MonoidAggregator[String, Long, Long] =
    AlbAggregator.fromMonoid(Monoid.longMonoid, (s: String) => s.length.toLong)

  "RichMonoidAggregator.filterNotNull" should {
    "pass non-null inputs to the underlying aggregator" in {
      val result = sumStrLen.filterNotNull.apply(Seq("hello", "world"))
      result shouldBe 10L
    }

    "drop null inputs before aggregation" in {
      val result = sumStrLen.filterNotNull.apply(Seq("hello", null, "world", null))
      result shouldBe 10L
    }

    "return the monoid zero when all inputs are null" in {
      val result = sumStrLen.filterNotNull.apply(Seq[String](null, null))
      result shouldBe 0L
    }

    "return the monoid zero on an empty input" in {
      val result = sumStrLen.filterNotNull.apply(Seq.empty[String])
      result shouldBe 0L
    }
  }
}
