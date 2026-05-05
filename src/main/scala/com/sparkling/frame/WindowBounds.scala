package com.sparkling.frame

import org.apache.spark.sql.expressions.{Window => SparkWindow}

/** Frame bounds for aggregate window operations.
  *
  * Use [[WindowBounds.rowsBetween]] or [[WindowBounds.rangeBetween]] to construct bounds, and the sentinel constants
  * [[WindowBounds.unboundedPreceding]], [[WindowBounds.currentRow]], and [[WindowBounds.unboundedFollowing]] as
  * boundary values.
  *
  * Example:
  * {{{
  * import com.sparkling.frame.WindowBounds
  * import com.sparkling.frame.WindowBounds._
  *
  * rowsBetween(unboundedPreceding, currentRow)          // running total (the default)
  * rowsBetween(-6, 0)                                   // 7-row rolling window
  * rangeBetween(unboundedPreceding, unboundedFollowing) // whole partition
  * }}}
  */
sealed trait WindowBounds

object WindowBounds {

  final private[frame] case class Rows(start: Long, end: Long) extends WindowBounds
  final private[frame] case class Range(start: Long, end: Long) extends WindowBounds

  /** All rows before the current row in the partition. */
  val unboundedPreceding: Long = SparkWindow.unboundedPreceding

  /** The current row. */
  val currentRow: Long = SparkWindow.currentRow

  /** All rows after the current row in the partition. */
  val unboundedFollowing: Long = SparkWindow.unboundedFollowing

  /** Physical row-based frame: includes rows at physical offsets `[start, end]` relative to the current row. */
  def rowsBetween(start: Long, end: Long): WindowBounds = Rows(start, end)

  /** Value-range frame: includes rows whose sort-key value falls within `[current + start, current + end]`. */
  def rangeBetween(start: Long, end: Long): WindowBounds = Range(start, end)

  /** Running total: from the start of the partition to the current row. Default for aggregate window ops. */
  val runningTotal: WindowBounds = Rows(SparkWindow.unboundedPreceding, SparkWindow.currentRow)
}
