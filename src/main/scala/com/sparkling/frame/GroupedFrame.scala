package com.sparkling.frame

import org.apache.spark.sql.{functions => sqlf, Column}

import com.sparkling.schema.{Field, Fields}

/** A planned aggregation: an output column name paired with its Spark SQL aggregation expression. */
sealed trait PlannedAgg {
  def outName: String
  def expr: Column
}

object PlannedAgg {

  /** A single-output aggregation that writes one expression into one output column.
    *
    * @param outName name of the output column
    * @param expr Spark SQL aggregation expression
    */
  final case class Direct(outName: String, expr: Column) extends PlannedAgg
}

/** Builder for grouped aggregations over a [[Frame]].
  *
  * Constructed via [[Frame.groupBy]] or [[Frame.groupAll]]; not instantiated directly. Each method returns a new
  * `GroupedFrame`, leaving the receiver unchanged. Call [[Frame.groupBy]] with a function to produce the final
  * aggregated [[Frame]].
  *
  * Example:
  * {{{
  * frame.groupBy(Fields("dept")) {
  *   _.count(Field("n"))
  *    .avg(Fields("salary") -> Field("avg_salary"))
  * }
  * }}}
  *
  * @param frame underlying Frame to aggregate
  * @param keys grouping key columns; empty means aggregate all rows into one
  * @param aggregations planned aggregation expressions in declaration order
  * @param postMaps transformations applied to the aggregated Frame before returning
  * @param maybePivot optional pivot column and enumerated values
  */
final case class GroupedFrame(
    frame: Frame,
    keys: Fields,
    aggregations: Vector[PlannedAgg],
    postMaps: Vector[Frame => Frame],
    maybePivot: Option[(Field, Fields)]
) {

  // ── Internal helpers ────────────────────────────────────────────────────────

  /** Appends a single `PlannedAgg.Direct` to the aggregation plan. */
  private def addAgg(outName: String, expr: Column): GroupedFrame =
    copy(aggregations = aggregations :+ PlannedAgg.Direct(outName, expr))

  /** Appends a single unary aggregation: one input column, one output column, one aggregation function. */
  private def unaryAgg(fs: (Fields, Field), aggFn: Column => Column): GroupedFrame = {
    val (in, out) = fs
    require(in.isSingle, s"unary aggregation requires exactly one input field (got ${in.size})")
    addAgg(out.name, aggFn(sqlf.col(in.names.head)))
  }

  // ── Aggregations ────────────────────────────────────────────────────────────

  /** Counts rows in each group, writing the result to `out`.
    *
    * @param out output column for the row count
    */
  def count(out: Field): GroupedFrame =
    addAgg(out.name, sqlf.count(sqlf.lit(1)))

  /** Computes the mean of the input column, writing the result to `out`.
    *
    * @param fs mapping from a single input column to the output column
    */
  def avg(fs: (Fields, Field)): GroupedFrame =
    unaryAgg(fs, sqlf.avg)

  /** Computes the minimum of the input column, writing the result to `out`.
    *
    * @param fs mapping from a single input column to the output column
    */
  def min(fs: (Fields, Field)): GroupedFrame =
    unaryAgg(fs, sqlf.min)

  /** Computes the maximum of the input column, writing the result to `out`.
    *
    * @param fs mapping from a single input column to the output column
    */
  def max(fs: (Fields, Field)): GroupedFrame =
    unaryAgg(fs, sqlf.max)

  /** Computes the sum of the input column, writing the result to `out`.
    *
    * @param fs mapping from a single input column to the output column
    */
  def sum(fs: (Fields, Field)): GroupedFrame =
    unaryAgg(fs, sqlf.sum)

  /** Computes the population standard deviation of the input column, writing the result to `out`.
    *
    * @param fs mapping from a single input column to the output column
    */
  def stddevPop(fs: (Fields, Field)): GroupedFrame =
    unaryAgg(fs, sqlf.stddev_pop)

  /** Computes the sample standard deviation of the input column, writing the result to `out`.
    *
    * @param fs mapping from a single input column to the output column
    */
  def stddevSamp(fs: (Fields, Field)): GroupedFrame =
    unaryAgg(fs, sqlf.stddev_samp)

  /** Computes the population variance of the input column, writing the result to `out`.
    *
    * @param fs mapping from a single input column to the output column
    */
  def variancePop(fs: (Fields, Field)): GroupedFrame =
    unaryAgg(fs, sqlf.var_pop)

  /** Computes the sample variance of the input column, writing the result to `out`.
    *
    * @param fs mapping from a single input column to the output column
    */
  def varianceSamp(fs: (Fields, Field)): GroupedFrame =
    unaryAgg(fs, sqlf.var_samp)

  /** Counts the number of distinct values in the input column, writing the result to `out`.
    *
    * @param fs mapping from a single input column to the output column
    */
  def countDistinct(fs: (Fields, Field)): GroupedFrame =
    unaryAgg(fs, sqlf.count_distinct(_))

  /** Approximates the number of distinct values using HyperLogLog++, writing the result to `out`.
    *
    * @param fs mapping from a single input column to the output column
    * @param rsd maximum relative standard deviation (default `0.05`)
    */
  def approxCountDistinct(fs: (Fields, Field), rsd: Double = 0.05): GroupedFrame = {
    val (in, out) = fs
    require(in.isSingle, s"approxCountDistinct requires exactly one input field (got ${in.size})")
    addAgg(out.name, sqlf.approx_count_distinct(sqlf.col(in.names.head), rsd))
  }

  /** Computes approximate percentile(s) of the input column using the Greenwald-Khanna algorithm.
    *
    * @param fs mapping from a single input column to the output column
    * @param percentage percentile(s) to compute, each in `[0.0, 1.0]`; a single value produces a scalar output while
    *   multiple values produce an array output
    * @param accuracy controls the approximation trade-off: higher values improve accuracy at the cost of memory; must
    *   be a positive integer (default `10000`)
    */
  def approxPercentile(
      fs: (Fields, Field),
      percentage: Seq[Double],
      accuracy: Int = 10000
  ): GroupedFrame = {
    val (in, out) = fs
    require(in.isSingle, s"approxPercentile requires exactly one input field (got ${in.size})")
    require(percentage.nonEmpty, "approxPercentile requires at least one percentage value")
    val inCol = sqlf.col(in.names.head)
    val expr =
      if (percentage.size == 1)
        sqlf.percentile_approx(inCol, sqlf.lit(percentage.head), sqlf.lit(accuracy))
      else
        sqlf.percentile_approx(
          inCol,
          sqlf.array(percentage.map(sqlf.lit): _*),
          sqlf.lit(accuracy)
        )
    addAgg(out.name, expr)
  }

  /** Returns the first value of the input column within each group, writing the result to `out`.
    *
    * @param fs mapping from a single input column to the output column
    * @param ignoreNulls if true, skips null values (default false)
    */
  def first(fs: (Fields, Field), ignoreNulls: Boolean = false): GroupedFrame = {
    val (in, out) = fs
    require(in.isSingle, s"first requires exactly one input field (got ${in.size})")
    addAgg(out.name, sqlf.first(sqlf.col(in.names.head), ignoreNulls))
  }

  /** Returns the last value of the input column within each group, writing the result to `out`.
    *
    * @param fs mapping from a single input column to the output column
    * @param ignoreNulls if true, skips null values (default false)
    */
  def last(fs: (Fields, Field), ignoreNulls: Boolean = false): GroupedFrame = {
    val (in, out) = fs
    require(in.isSingle, s"last requires exactly one input field (got ${in.size})")
    addAgg(out.name, sqlf.last(sqlf.col(in.names.head), ignoreNulls))
  }

  /** Collects values of the input column into an array, writing the result to `out`.
    *
    * @note The result array is unordered. Use [[postMap]] to sort within-group if needed.
    * @param fs mapping from a single input column to the output column
    */
  def collectList(fs: (Fields, Field)): GroupedFrame =
    unaryAgg(fs, sqlf.collect_list)

  /** Collects distinct values of the input column into an array, writing the result to `out`.
    *
    * @note The result array is unordered.
    * @param fs mapping from a single input column to the output column
    */
  def collectSet(fs: (Fields, Field)): GroupedFrame =
    unaryAgg(fs, sqlf.collect_set)

  /** Adds a single user-supplied aggregation expression, writing the result to `out`.
    *
    * @param out output column for the aggregation result
    * @param aggExpr Spark SQL aggregation expression
    */
  def expr(out: Field, aggExpr: Column): GroupedFrame =
    addAgg(out.name, aggExpr)

  /** Adds multiple user-supplied aggregation expressions in a single call.
    *
    * @param exprs sequence of (output field, aggregation expression) pairs (must be non-empty)
    * @throws java.lang.IllegalArgumentException if `exprs` is empty
    */
  def exprs(exprs: Seq[(Field, Column)]): GroupedFrame = {
    require(exprs.nonEmpty, "exprs requires at least one aggregation")
    exprs.foldLeft(this) { case (g, (out, aggExpr)) => g.addAgg(out.name, aggExpr) }
  }

  // ── Pivot ───────────────────────────────────────────────────────────────────

  /** Pivots on a column, optionally specifying the enumerated values to pivot over.
    *
    * When `values` is non-empty, Spark generates one output column per listed value (avoiding a costly distinct pass
    * over the data). When `values` is empty, Spark infers the distinct values automatically.
    *
    * @param field column to pivot on
    * @param values enumerated pivot values; empty means infer from data (default)
    */
  def pivot(field: Field, values: Fields = Fields.empty): GroupedFrame = {
    require(maybePivot.isEmpty, "pivot may only be called once per GroupedFrame")
    copy(maybePivot = Some((field, values)))
  }

  // ── Post-processing ─────────────────────────────────────────────────────────

  /** Applies an additional transformation to the aggregated [[Frame]] before it is returned.
    *
    * Multiple calls to `postMap` are composed in order.
    *
    * @param f transformation to apply after aggregation
    */
  def postMap(f: Frame => Frame): GroupedFrame =
    copy(postMaps = postMaps :+ f)

  // ── Execution ───────────────────────────────────────────────────────────────

  /** Executes the planned aggregation and returns the result as a [[Frame]].
    *
    * Called internally by [[Frame.groupBy]] and [[Frame.groupAll]].
    */
  private[frame] def run(): Frame = {
    require(aggregations.nonEmpty, "no aggregations planned")

    val aggExprs: IndexedSeq[Column] = aggregations.map(a => a.expr.as(a.outName)).toIndexedSeq

    val grouped =
      if (keys.isEmpty) frame.df.groupBy()
      else frame.df.groupBy(keys.names.map(sqlf.col): _*)

    val pivotedAndAgged = maybePivot match {
      case None =>
        grouped.agg(aggExprs.head, aggExprs.tail: _*)

      case Some((pivotField, pivotValues)) =>
        val pivoted =
          if (pivotValues.isEmpty) grouped.pivot(pivotField.name)
          else grouped.pivot(pivotField.name, pivotValues.names)
        pivoted.agg(aggExprs.head, aggExprs.tail: _*)
    }

    val aggregated = Frame(pivotedAndAgged)
    postMaps.foldLeft(aggregated)((fr, f) => f(fr))
  }
}
