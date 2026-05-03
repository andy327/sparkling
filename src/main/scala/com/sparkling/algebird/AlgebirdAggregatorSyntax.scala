package com.sparkling.algebird

import scala.reflect.runtime.universe.TypeTag

import com.twitter.algebird.{Monoid, MonoidAggregator}
import org.apache.spark.sql.expressions.{Aggregator => SqlAggregator}
import org.apache.spark.sql.{functions => sqlf, Column, Encoder}

import com.sparkling.evidence.EncoderEvidence

/** Adapters for running Algebird aggregations inside Spark SQL.
  *
  * Two integration styles:
  *
  *   1. Typed aggregators: [[monoidToSql]] and [[algebirdToSql]] return a Spark `Aggregator[A, B, C]`, appropriate when
  *      the buffer type `B` is SQL-encodable (has a usable `Encoder[B]`).
  *
  *   2. UDAF column with binary buffer: [[algebirdToUdafCol]] wraps a `MonoidAggregator` in an
  *      [[MonoidAggregatorUdaf]] whose Spark buffer type is `Array[Byte]`. This allows non-encodable Algebird
  *      buffers (sketches, mutable structures) to be shuffled safely, without using any Catalyst internals.
  */
object AlgebirdAggregatorSyntax {

  /** Wraps an Algebird `Monoid` as a Spark typed `Aggregator[A, B, B]`.
    *
    * Each input `A` is mapped to `B` via `toB`, combined with `Monoid.plus`, and the accumulated `B` is emitted.
    *
    * @param toB maps each input element to its monoid contribution
    */
  def monoidToSql[A, B](toB: A => B)(implicit M: Monoid[B], bEv: EncoderEvidence[B]): SqlAggregator[A, B, B] =
    algebirdToSql(new MonoidAggregator[A, B, B] {
      val monoid: Monoid[B] = M
      def prepare(a: A): B = toB(a)
      def present(b: B): B = b
    })

  /** Wraps an Algebird `MonoidAggregator` as a Spark typed `Aggregator[A, B, C]`.
    *
    * Appropriate when `B` is SQL-encodable and you want Spark to handle buffer persistence using that encoder.
    * If `B` is not SQL-encodable, use [[algebirdToUdafCol]] instead.
    */
  def algebirdToSql[A, B, C](
      alg: MonoidAggregator[A, B, C]
  )(implicit bEv: EncoderEvidence[B], cEv: EncoderEvidence[C]): SqlAggregator[A, B, C] =
    new SqlAggregator[A, B, C] {
      override def zero: B = alg.monoid.zero
      override def reduce(b: B, a: A): B = alg.monoid.plus(b, alg.prepare(a))
      override def merge(b1: B, b2: B): B = alg.monoid.plus(b1, b2)
      override def finish(r: B): C = alg.present(r)
      override def bufferEncoder: Encoder[B] = bEv.encoder
      override def outputEncoder: Encoder[C] = cEv.encoder
    }

  /** Builds a Spark SQL aggregate `Column` for a `MonoidAggregator` using binary buffer serialization.
    *
    * The Algebird buffer `B` does not need a Spark `Encoder` — it is serialized to `Array[Byte]` via [[BufferSerDe]].
    * Only the output type `C` requires an `Encoder`.
    *
    * @param inputColName name of the single input column to aggregate
    * @param alg Algebird monoid aggregator
    */
  def algebirdToUdafCol[A: TypeTag, B, C](
      inputColName: String,
      alg: MonoidAggregator[A, B, C]
  )(implicit
      bSerDe: BufferSerDe[B],
      cEnc: Encoder[C]
  ): Column = {
    val udaf = new MonoidAggregatorUdaf[A, B, C](alg)
    sqlf.udaf(udaf).apply(sqlf.col(inputColName))
  }
}
