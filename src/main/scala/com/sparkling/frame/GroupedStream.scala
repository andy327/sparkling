package com.sparkling.frame

import com.sparkling.row.convert.{RowDecoder, RowEncoder}
import com.sparkling.schema.Fields

/** Immutable plan builder for reduce-side streaming group operations.
  *
  * A `GroupedStream` is created via [[Frame.streamBy]] or [[Frame.streamAll]] and is fully immutable: each method
  * returns a new instance. Build the plan by:
  *   1. Optionally configuring sort order with [[sortBy]] and [[reverse]].
  *   2. Defining exactly one stream operation via [[mapGroups]] or [[mapStreamWithContext]].
  *
  * The plan is executed when [[Frame.streamBy]] / [[Frame.streamAll]] call `f(gs).run()` internally.
  *
  * @param frame underlying Frame
  * @param keys grouping key fields (empty means all rows form one group)
  * @param maybeSort optional sort configuration: (sort fields, descending flag)
  * @param maybeOp optional stream operation (must be set exactly once before running)
  */
final case class GroupedStream private[frame] (
    private val frame: Frame,
    private val keys: Fields,
    private val maybeSort: Option[(Fields, Boolean)],
    private val maybeOp: Option[StreamOperation]
) {

  /** Specifies secondary-sort fields for ordering rows within each group.
    *
    * Must be called before defining the stream operation. Ascending by default; use [[reverse]] to flip.
    *
    * @param fields sort fields (must be non-empty)
    * @throws java.lang.IllegalArgumentException if fields are empty or sort is already configured
    */
  def sortBy(fields: Fields): GroupedStream = {
    require(fields.nonEmpty, "sortBy requires at least one field")
    require(maybeSort.isEmpty, "sortBy: sort already configured")
    copy(maybeSort = Some((fields, false)))
  }

  /** Reverses the sort order set by [[sortBy]] (descending instead of ascending).
    *
    * @throws java.lang.IllegalArgumentException if [[sortBy]] has not been called yet
    */
  def reverse: GroupedStream = {
    require(maybeSort.isDefined, "reverse: first set fields to sort on (sortBy)")
    copy(maybeSort = maybeSort.map { case (fs, _) => (fs, true) })
  }

  /** Defines a context-free stream operation that maps an input iterator to an output iterator.
    *
    * Each group receives an iterator over its (optionally sorted) input rows decoded into `I`, and the function must
    * return an iterator of output values `O`. Zero or more output rows are emitted per group.
    *
    * @param fs mapping from input fields to output fields (both sides must be non-empty)
    * @param f function mapping a group's typed input iterator to a typed output iterator
    * @throws java.lang.IllegalArgumentException if input or output fields are empty
    */
  def mapGroups[I, O](fs: (Fields, Fields))(f: Iterator[I] => Iterator[O])(implicit
      dec: RowDecoder[I],
      enc: RowEncoder[O]
  ): GroupedStream = {
    val (in, out) = fs
    require(in.nonEmpty, "mapGroups input fields must be non-empty")
    require(out.nonEmpty, "mapGroups output fields must be non-empty")
    copy(maybeOp = Some(StreamOperation.mapGroups(fs)(f)))
  }

  /** Defines a stateful stream operation that threads a per-group context through the iterator.
    *
    * The context is freshly initialized for each group via the call-by-name `init` block. The `step` function
    * receives the context and the group's typed input iterator and must return an output iterator.
    *
    * @param fs mapping from input fields to output fields (both sides must be non-empty)
    * @param init call-by-name expression producing a fresh per-group context
    * @param step function receiving the per-group context and input iterator, returning the output iterator
    * @throws java.lang.IllegalArgumentException if input or output fields are empty
    */
  def mapStreamWithContext[I, O, C](fs: (Fields, Fields))(init: => C)(
      step: (C, Iterator[I]) => Iterator[O]
  )(implicit dec: RowDecoder[I], enc: RowEncoder[O]): GroupedStream = {
    val (in, out) = fs
    require(in.nonEmpty, "mapStreamWithContext input fields must be non-empty")
    require(out.nonEmpty, "mapStreamWithContext output fields must be non-empty")
    copy(maybeOp = Some(StreamOperation.mapStreamWithContext(fs)(init)(step)))
  }

  /** Executes the stream plan and returns the resulting [[Frame]].
    *
    * @throws java.lang.IllegalArgumentException if no stream operation has been defined
    */
  private[frame] def run(): Frame = {
    require(
      maybeOp.isDefined,
      "GroupedStream requires exactly one stream operation (mapGroups or mapStreamWithContext)"
    )
    Frame(StreamExecutor.run(frame.df, keys, maybeOp.get, maybeSort))
  }
}
