package com.sparkling.algebird

import com.twitter.algebird.MonoidAggregator

/** Syntax enrichment for Algebird types. */
trait AlgebirdSyntax {

  implicit final class RichMonoidAggregator[A, B, C](private val ma: MonoidAggregator[A, B, C]) {

    /** Ignores null inputs before running the underlying aggregator. */
    def filterNotNull: MonoidAggregator[A, B, C] =
      ma.filterBefore((a: A) => a != null)
  }
}
