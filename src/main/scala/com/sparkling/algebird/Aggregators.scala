package com.sparkling.algebird

import com.twitter.algebird.{Aggregator, MonoidAggregator, SSOne, SpaceSaver, SpaceSaverSemigroup}

/** Result row for a SpaceSaver top-k aggregation.
  *
  * @param item the item in the top-k
  * @param min lower bound on the estimated count (from SpaceSaver)
  * @param max upper bound on the estimated count (from SpaceSaver)
  */
final case class TopKResult[T](item: T, min: Long, max: Long)

object Aggregators {

  /** Builds a SpaceSaver-based top-k `MonoidAggregator` that returns min/max count bounds.
    *
    * The aggregator's buffer type `Option[SpaceSaver[T]]` is not SQL-encodable; use via
    * [[com.sparkling.frame.GroupedFrame.aggregate]] or [[com.sparkling.frame.GroupedFrame.aggregatePacked]], which
    * route Kryo buffers through [[MonoidAggregatorUdaf]] automatically.
    *
    * @param capacity SpaceSaver internal sketch size (memory/accuracy tradeoff)
    * @param k number of top elements to return
    */
  def forSpaceSaver[T](capacity: Int, k: Int): MonoidAggregator[T, Option[SpaceSaver[T]], Seq[TopKResult[T]]] = {
    require(k > 0, s"k must be > 0 (got $k)")
    require(capacity > k, s"capacity must be > k (got capacity=$capacity, k=$k)")

    Aggregator
      .fromSemigroup(new SpaceSaverSemigroup[T])
      .composePrepare(SSOne(capacity, _: T))
      .lift
      .andThenPresent { opt =>
        opt.toSeq.flatMap(_.topK(k).map { case (item, approx, _) =>
          TopKResult(item, approx.min, approx.max)
        })
      }
  }
}
