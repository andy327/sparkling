package com.sparkling.algebird

import com.twitter.algebird.MonoidAggregator
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.{Encoder, Encoders}

/** Wraps an Algebird `MonoidAggregator[A, B, C]` as a Spark typed `Aggregator[A, Array[Byte], C]`.
  *
  * The Algebird buffer `B` is serialized to/from `Array[Byte]` via [[BufferSerDe]], so `B` does not require a Spark
  * `Encoder`. Spark treats the buffer as a SQL-encodable binary column and can shuffle it safely.
  *
  * @param alg Algebird monoid aggregator to run
  */
final class MonoidAggregatorUdaf[A, B, C](
    alg: MonoidAggregator[A, B, C]
)(implicit
    bSerDe: BufferSerDe[B],
    cEnc: Encoder[C]
) extends Aggregator[A, Array[Byte], C] {

  private def ser(b: B): Array[Byte] = bSerDe.toBytes(b)
  private def deser(bs: Array[Byte]): B = bSerDe.fromBytes(bs)

  override def zero: Array[Byte] = ser(alg.monoid.zero)

  override def reduce(bufBytes: Array[Byte], a: A): Array[Byte] =
    ser(alg.monoid.plus(deser(bufBytes), alg.prepare(a)))

  override def merge(b1: Array[Byte], b2: Array[Byte]): Array[Byte] =
    ser(alg.monoid.plus(deser(b1), deser(b2)))

  override def finish(reductionBytes: Array[Byte]): C =
    alg.present(deser(reductionBytes))

  override def bufferEncoder: Encoder[Array[Byte]] = Encoders.BINARY
  override def outputEncoder: Encoder[C] = cEnc
}
