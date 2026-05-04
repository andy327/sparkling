package com.sparkling.frame

import com.sparkling.row.convert.{RowDecoder, RowEncoder}
import com.sparkling.schema.Fields

/** A reduce-side (stream) group operation.
  *
  * A [[StreamOperation]] is executed after grouping, where Spark provides an `Iterator` of values per group and user
  * code may emit 0+ output rows per group. This is the plan-level object stored by [[GroupedStream]] for stream plans.
  *
  * Notes:
  *   - `in` / `out` are the logical fields to decode/encode for the value stream, not including group keys.
  *   - Group keys are appended by the executor when producing final output rows (final schema is typically
  *     `keys ++ out`).
  *   - Sorting is configured separately on [[GroupedStream]] and applied by the stream execution path.
  */
sealed trait StreamOperation extends Product with Serializable {

  /** Input element type decoded from each row in the group iterator. */
  type In

  /** Output element type encoded to rows emitted from the group iterator. */
  type Out

  /** Per-group context type (use `Unit` for context-free operations). */
  type Ctx

  /** Logical fields read from each grouped value row. */
  def in: Fields

  /** Logical fields written for each emitted output row (keys are appended separately). */
  def out: Fields

  /** Decoder used to decode grouped value rows into `In`. */
  implicit def dec: RowDecoder[In]

  /** Encoder used to encode emitted values back into rows. */
  implicit def enc: RowEncoder[Out]

  /** Builds a fresh per-group context. Called once per group by the executor. */
  def init(): Ctx

  /** Runs the operation over the grouped iterator, returning 0+ outputs for the group. */
  def step(ctx: Ctx, iter: Iterator[In]): Iterator[Out]
}

object StreamOperation {

  /** Context-free stream op: maps an input iterator to an output iterator. */
  final case class MapGroups[I, O](in: Fields, out: Fields, f: Iterator[I] => Iterator[O])(implicit
      val dec: RowDecoder[I],
      val enc: RowEncoder[O]
  ) extends StreamOperation {

    override type In = I
    override type Out = O
    override type Ctx = Unit

    override def init(): Unit = ()

    override def step(ctx: Unit, iter: Iterator[I]): Iterator[O] = f(iter)
  }

  /** Stateful stream op: threads a per-group context through the iterator processing. */
  final case class MapStreamWithContext[I, O, C](
      in: Fields,
      out: Fields,
      initFn: () => C,
      stepFn: (C, Iterator[I]) => Iterator[O]
  )(implicit val dec: RowDecoder[I], val enc: RowEncoder[O])
      extends StreamOperation {

    override type In = I
    override type Out = O
    override type Ctx = C

    override def init(): C = initFn()

    override def step(ctx: C, iter: Iterator[I]): Iterator[O] = stepFn(ctx, iter)
  }

  /** Convenience constructor for context-free stream ops. */
  def mapGroups[I, O](fs: (Fields, Fields))(
      f: Iterator[I] => Iterator[O]
  )(implicit dec: RowDecoder[I], enc: RowEncoder[O]): MapGroups[I, O] =
    MapGroups(fs._1, fs._2, f)

  /** Convenience constructor for stateful stream ops. */
  def mapStreamWithContext[I, O, C](fs: (Fields, Fields))(init: => C)(
      step: (C, Iterator[I]) => Iterator[O]
  )(implicit dec: RowDecoder[I], enc: RowEncoder[O]): MapStreamWithContext[I, O, C] =
    MapStreamWithContext(fs._1, fs._2, () => init, step)
}
