package com.sparkling.frame

import org.apache.spark.sql.expressions.{Window => SparkWindow, WindowSpec => SparkWindowSpec}
import org.apache.spark.sql.{functions => sqlf, Column}

import com.sparkling.schema.{Field, Fields}

/** Fluent builder for window function operations over a [[Frame]].
  *
  * Constructed via [[Frame.windowBy]] or [[Frame.windowAll]]. Call ordering and operation methods to describe the
  * window, then the result is applied when the enclosing block returns.
  *
  * Example:
  * {{{
  * frame.windowBy("dept") {
  *   _.orderBy("salary")
  *    .rank("dept_rank")
  *    .lag("salary" -> "prev_salary", offset = 1)
  *    .sum("salary" -> "running_total")
  * }
  * }}}
  *
  * @param frame underlying Frame
  * @param keys partition-by columns; empty means a global window (all rows in one partition)
  * @param maybeSort sort columns with per-column direction (`false` = ascending); `None` means unordered
  * @param ops accumulated window operations, each a function from a [[SparkWindowSpec]] to an (output name, Column)
  */
final case class WindowedFrame private[frame] (
    frame: Frame,
    keys: Fields,
    maybeSort: Option[Vector[(Field, Boolean)]],
    ops: Vector[SparkWindowSpec => (String, Column)]
) {

  // ── Ordering ──────────────────────────────────────────────────────────────

  /** Sets the window ordering with a uniform direction (ascending by default).
    *
    * May be called multiple times; sort fields accumulate in declaration order.
    *
    * @param fs columns to sort by (must be non-empty)
    * @param descending if true, sorts all given columns descending
    * @throws java.lang.IllegalArgumentException if `fs` is empty
    */
  def orderBy(fs: Fields, descending: Boolean = false): WindowedFrame = {
    require(fs.nonEmpty, "orderBy requires at least one field")
    val pairs = fs.names.map(n => Field(n) -> descending).toVector
    copy(maybeSort = Some(maybeSort.getOrElse(Vector.empty) ++ pairs))
  }

  /** Sets the window ordering with explicit per-column directions.
    *
    * Each pair is `(columnName, descending)`. May be called multiple times; sort fields accumulate.
    *
    * @param fs sequence of `(columnName, descending)` pairs (must be non-empty)
    * @throws java.lang.IllegalArgumentException if `fs` is empty
    */
  def orderBy(fs: Seq[(String, Boolean)]): WindowedFrame = {
    require(fs.nonEmpty, "orderBy requires at least one field")
    val pairs = fs.map { case (name, desc) => Field(name) -> desc }.toVector
    copy(maybeSort = Some(maybeSort.getOrElse(Vector.empty) ++ pairs))
  }

  // ── Ranking ───────────────────────────────────────────────────────────────

  /** Assigns a rank within each partition. Equal values share the same rank; the next rank skips accordingly.
    *
    * Requires [[orderBy]] to be set.
    *
    * @param out output column to write the rank into
    */
  def rank(out: Field): WindowedFrame =
    addOp(ws => out.name -> sqlf.rank().over(ws))

  /** Assigns a dense rank within each partition. Equal values share the same rank; no ranks are skipped.
    *
    * Requires [[orderBy]] to be set.
    *
    * @param out output column to write the rank into
    */
  def denseRank(out: Field): WindowedFrame =
    addOp(ws => out.name -> sqlf.dense_rank().over(ws))

  /** Assigns a unique sequential row number within each partition, starting at 1.
    *
    * Requires [[orderBy]] to be set.
    *
    * @param out output column to write the row number into
    */
  def rowNumber(out: Field): WindowedFrame =
    addOp(ws => out.name -> sqlf.row_number().over(ws))

  /** Divides each partition into `n` equal-sized buckets and assigns each row a bucket number (1 to `n`).
    *
    * Requires [[orderBy]] to be set.
    *
    * @param n number of buckets (must be positive)
    * @param out output column to write the bucket number into
    */
  def ntile(n: Int, out: Field): WindowedFrame =
    addOp(ws => out.name -> sqlf.ntile(n).over(ws))

  /** Assigns a percent rank within each partition: `(rank - 1) / (total rows - 1)`.
    *
    * Requires [[orderBy]] to be set.
    *
    * @param out output column to write the percent rank into
    */
  def percentRank(out: Field): WindowedFrame =
    addOp(ws => out.name -> sqlf.percent_rank().over(ws))

  // ── Analytic ──────────────────────────────────────────────────────────────

  /** Returns the value of the input column `offset` rows before the current row within the partition.
    *
    * Returns null when the offset falls before the partition boundary. Requires [[orderBy]] to be set.
    *
    * @param fs input → output field mapping (single field on each side)
    * @param offset number of rows to look back (default 1)
    * @throws java.lang.IllegalArgumentException if either side of `fs` is not a single field
    */
  def lag(fs: (Fields, Fields), offset: Int = 1): WindowedFrame = {
    val (in, out) = toFieldPair(fs)
    addOp(ws => out.name -> sqlf.lag(sqlf.col(in.name), offset).over(ws))
  }

  /** Returns the value of the input column `offset` rows after the current row within the partition.
    *
    * Returns null when the offset falls after the partition boundary. Requires [[orderBy]] to be set.
    *
    * @param fs input → output field mapping (single field on each side)
    * @param offset number of rows to look ahead (default 1)
    * @throws java.lang.IllegalArgumentException if either side of `fs` is not a single field
    */
  def lead(fs: (Fields, Fields), offset: Int = 1): WindowedFrame = {
    val (in, out) = toFieldPair(fs)
    addOp(ws => out.name -> sqlf.lead(sqlf.col(in.name), offset).over(ws))
  }

  // ── Aggregate ─────────────────────────────────────────────────────────────

  /** Computes the running sum of the input column over the window.
    *
    * @param fs input → output field mapping (single field on each side)
    * @param bounds frame bounds (default: running total from partition start to current row)
    * @throws java.lang.IllegalArgumentException if either side of `fs` is not a single field
    */
  def sum(fs: (Fields, Fields), bounds: WindowBounds = WindowBounds.runningTotal): WindowedFrame = {
    val (in, out) = toFieldPair(fs)
    addOp(ws => out.name -> sqlf.sum(sqlf.col(in.name)).over(applyBounds(ws, bounds)))
  }

  /** Computes the running average of the input column over the window.
    *
    * @param fs input → output field mapping (single field on each side)
    * @param bounds frame bounds (default: running total from partition start to current row)
    * @throws java.lang.IllegalArgumentException if either side of `fs` is not a single field
    */
  def avg(fs: (Fields, Fields), bounds: WindowBounds = WindowBounds.runningTotal): WindowedFrame = {
    val (in, out) = toFieldPair(fs)
    addOp(ws => out.name -> sqlf.avg(sqlf.col(in.name)).over(applyBounds(ws, bounds)))
  }

  /** Computes the running minimum of the input column over the window.
    *
    * @param fs input → output field mapping (single field on each side)
    * @param bounds frame bounds (default: running total from partition start to current row)
    * @throws java.lang.IllegalArgumentException if either side of `fs` is not a single field
    */
  def min(fs: (Fields, Fields), bounds: WindowBounds = WindowBounds.runningTotal): WindowedFrame = {
    val (in, out) = toFieldPair(fs)
    addOp(ws => out.name -> sqlf.min(sqlf.col(in.name)).over(applyBounds(ws, bounds)))
  }

  /** Computes the running maximum of the input column over the window.
    *
    * @param fs input → output field mapping (single field on each side)
    * @param bounds frame bounds (default: running total from partition start to current row)
    * @throws java.lang.IllegalArgumentException if either side of `fs` is not a single field
    */
  def max(fs: (Fields, Fields), bounds: WindowBounds = WindowBounds.runningTotal): WindowedFrame = {
    val (in, out) = toFieldPair(fs)
    addOp(ws => out.name -> sqlf.max(sqlf.col(in.name)).over(applyBounds(ws, bounds)))
  }

  /** Counts non-null values of the input column over the window.
    *
    * @param fs input → output field mapping (single field on each side)
    * @param bounds frame bounds (default: running total from partition start to current row)
    * @throws java.lang.IllegalArgumentException if either side of `fs` is not a single field
    */
  def count(fs: (Fields, Fields), bounds: WindowBounds = WindowBounds.runningTotal): WindowedFrame = {
    val (in, out) = toFieldPair(fs)
    addOp(ws => out.name -> sqlf.count(sqlf.col(in.name)).over(applyBounds(ws, bounds)))
  }

  // ── Internal ──────────────────────────────────────────────────────────────

  private[frame] def run(): Frame = {
    require(ops.nonEmpty, "windowBy requires at least one window operation")

    val partitioned =
      if (keys.nonEmpty) SparkWindow.partitionBy(keys.names.map(sqlf.col): _*)
      else SparkWindow.partitionBy()

    val windowSpec = maybeSort.fold(partitioned) { sortCols =>
      partitioned.orderBy(sortCols.map { case (f, desc) =>
        if (desc) sqlf.col(f.name).desc else sqlf.col(f.name)
      }: _*)
    }

    val resultDf = ops.foldLeft(frame.df) { case (acc, op) =>
      val (name, col) = op(windowSpec)
      acc.withColumn(name, col)
    }

    Frame(resultDf)
  }

  private def addOp(op: SparkWindowSpec => (String, Column)): WindowedFrame =
    copy(ops = ops :+ op)

  private def applyBounds(ws: SparkWindowSpec, bounds: WindowBounds): SparkWindowSpec =
    bounds match {
      case WindowBounds.Rows(s, e)  => ws.rowsBetween(s, e)
      case WindowBounds.Range(s, e) => ws.rangeBetween(s, e)
    }

  private def toFieldPair(fs: (Fields, Fields)): (Field, Field) = {
    val (in, out) = fs
    require(in.isSingle, s"expected single input field but got ${in.size}: ${in.names.mkString(", ")}")
    require(out.isSingle, s"expected single output field but got ${out.size}: ${out.names.mkString(", ")}")
    Field(in.names.head) -> Field(out.names.head)
  }
}
