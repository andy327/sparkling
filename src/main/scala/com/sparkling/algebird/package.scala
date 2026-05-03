package com.sparkling

import com.twitter.algebird.MonoidAggregator

package object algebird {

  implicit class RichMonoidAggregator[A, B, C](private val ma: MonoidAggregator[A, B, C]) extends AnyVal {

    /** Ignores null inputs before running the underlying aggregator. */
    def filterNotNull: MonoidAggregator[A, B, C] =
      ma.filterBefore((a: A) => a != null)
  }
}
