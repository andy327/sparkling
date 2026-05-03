package com.sparkling.frame

import scala.annotation.unused
import scala.reflect.ClassTag
import scala.reflect.runtime.universe.TypeTag

import com.twitter.algebird.MonoidAggregator
import org.apache.spark.sql.expressions.{Aggregator => SqlAggregator}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{functions => sqlf, Column, DataFrame, Encoder, Encoders}

import com.sparkling.algebird.AlgebirdAggregatorSyntax._
import com.sparkling.algebird.BufferSerDe
import com.sparkling.evidence.{EncoderEvidence, EncodingStrategy}
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

  /** A multi-output aggregation.
    *
    * The expression is computed into a temporary column `tmp` (typically a struct), then unpacked into the provided
    * output fields via `Frame.unpack`. The temporary column is dropped after unpacking.
    *
    * @param tmp temporary column used for the packed result
    * @param out output fields to unpack into
    * @param expr Spark aggregate expression producing a struct-like value
    */
  final case class Packed(tmp: Field, out: Fields, expr: Column) extends PlannedAgg {
    override def outName: String = tmp.name
    def unpack(df: DataFrame): DataFrame = Frame(df).unpack(tmp -> out).df
  }
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

  // ── Internal helpers ───────────────────────────────────────────────────────

  /** Appends a single `PlannedAgg.Direct` to the aggregation plan. */
  private def addAgg(outName: String, expr: Column): GroupedFrame =
    copy(aggregations = aggregations :+ PlannedAgg.Direct(outName, expr))

  /** Appends a single unary aggregation: one input column, one output column, one aggregation function. */
  private def unaryAgg(fs: (Fields, Field), aggFn: Column => Column): GroupedFrame = {
    val (in, out) = fs
    require(in.isSingle, s"unary aggregation requires exactly one input field (got ${in.size})")
    addAgg(out.name, aggFn(sqlf.col(in.names.head)))
  }

  /** Appends a [[PlannedAgg.Packed]] to the aggregation plan. */
  private def addPacked(tmp: Field, out: Fields, expr: Column): GroupedFrame =
    copy(aggregations = aggregations :+ PlannedAgg.Packed(tmp, out, expr))

  // ── Aggregations ───────────────────────────────────────────────────────────

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

  /** Returns the value of `valueField` from the row with the minimum `orderField` within each group.
    *
    * @param valueField column whose value to return
    * @param orderField column to minimize over
    * @param out output column for the result
    */
  def minBy(valueField: Field, orderField: Field, out: Field): GroupedFrame =
    addAgg(out.name, sqlf.min_by(sqlf.col(valueField.name), sqlf.col(orderField.name)))

  /** Returns the value of `valueField` from the row with the maximum `orderField` within each group.
    *
    * @param valueField column whose value to return
    * @param orderField column to maximize over
    * @param out output column for the result
    */
  def maxBy(valueField: Field, orderField: Field, out: Field): GroupedFrame =
    addAgg(out.name, sqlf.max_by(sqlf.col(valueField.name), sqlf.col(orderField.name)))

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

  // ── Typed / Algebird aggregations ──────────────────────────────────────────

  /** Entry point for typed single-output aggregations.
    *
    * {{{
    * _.aggregate(Fields("v") -> Fields("v_sum"))(sqlf.sum(sqlf.col("v")))
    * _.aggregate(Fields("v") -> Fields("v_sum"))(myMonoidAggregator)
    * }}}
    *
    * Output must be arity 1. Use [[aggregatePacked]] for multi-column outputs.
    *
    * @param fs input → output field mapping; output must be a single field
    */
  def aggregate(fs: (Fields, Fields)): AggregateBuilder = new AggregateBuilder(fs)

  /** Entry point for packed (multi-output) typed aggregations.
    *
    * The aggregator result is computed into a temporary struct column, then unpacked into the provided output fields via
    * `Frame.unpack`.
    *
    * {{{
    * _.aggregatePacked(Fields("v") -> Fields("v_sum", "v_max"))(mySqlAggregator)
    * }}}
    *
    * @param fs input → output field mapping
    */
  def aggregatePacked(fs: (Fields, Fields)): PackedAggregateBuilder = new PackedAggregateBuilder(fs)

  // ── Pivot ────────────────────────────────────────────────────────────────--

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

  // ── Post-processing ────────────────────────────────────────────────────────

  /** Applies an additional transformation to the aggregated [[Frame]] before it is returned.
    *
    * Multiple calls to `postMap` are composed in order.
    *
    * @param f transformation to apply after aggregation
    */
  def postMap(f: Frame => Frame): GroupedFrame =
    copy(postMaps = postMaps :+ f)

  // ── Execution ──────────────────────────────────────────────────────────────

  /** Executes the planned aggregation and returns the result as a [[Frame]].
    *
    * Called internally by [[Frame.groupBy]] and [[Frame.groupAll]].
    */
  private[frame] def run(): Frame = {
    require(aggregations.nonEmpty, "no aggregations planned")

    // Build one agg expression per planned agg; Packed uses its tmp name so it can be unpacked after.
    val aggCols: IndexedSeq[Column] = aggregations.map(a => a.expr.as(a.outName)).toIndexedSeq

    val grouped =
      if (keys.isEmpty) frame.df.groupBy()
      else frame.df.groupBy(keys.names.map(sqlf.col): _*)

    val pivotedAndAgged = maybePivot match {
      case None =>
        grouped.agg(aggCols.head, aggCols.tail: _*)

      case Some((pivotField, pivotValues)) =>
        val pivoted =
          if (pivotValues.isEmpty) grouped.pivot(pivotField.name)
          else grouped.pivot(pivotField.name, pivotValues.names)
        pivoted.agg(aggCols.head, aggCols.tail: _*)
    }

    val unpacked = aggregations.foldLeft(Frame(pivotedAndAgged)) { (fr, a) =>
      a match {
        case _: PlannedAgg.Direct => fr
        case p: PlannedAgg.Packed => Frame(p.unpack(fr.df))
      }
    }

    postMaps.foldLeft(unpacked)((fr, f) => f(fr))
  }

  // ── Builder inner classes ──────────────────────────────────────────────────

  /** Builder for single-output typed aggregations created by [[aggregate]].
    *
    * Call one of the `apply` overloads to choose the aggregation implementation.
    */
  final class AggregateBuilder private[frame] (fs: (Fields, Fields)) {

    /** Aggregates using a raw Spark SQL column expression. Output must be arity 1. */
    def apply(expr: Column): GroupedFrame = {
      val (_, out) = fs
      require(out.nonEmpty, "aggregate requires at least one output field")
      require(out.isSingle, s"aggregate(Column) requires arity 1 output (got ${out.size})")
      copy(aggregations = GroupedFrame.this.aggregations :+ PlannedAgg.Direct(out.names.head, expr))
    }

    /** Aggregates using a typed Spark `Aggregator[A, B, C]`.
      *
      * Multi-column input is supported (n → 1). If the input is a single struct column and `A` is a `Product`, the
      * struct sub-fields are expanded into multiple input columns automatically.
      *
      * If `C` is a `Product`, a wrapping workaround is applied to avoid Spark nullable-product issues.
      */
    def apply[A: TypeTag: ClassTag, B, C: ClassTag](
        sqlAgg: SqlAggregator[A, B, C]
    )(implicit
        @unused bEv: EncoderEvidence[B],
        @unused cEv: EncoderEvidence[C],
        @unused cTag: TypeTag[C]
    ): GroupedFrame = {
      val (in, out) = fs
      require(in.nonEmpty, "aggregate requires at least one input field")
      require(out.nonEmpty, "aggregate requires at least one output field")
      require(out.isSingle, s"aggregate(Aggregator) requires arity * -> 1 output (got ${out.size})")

      val inputCols = resolveInputCols[A](in)
      val cIsProduct = classOf[Product].isAssignableFrom(implicitly[ClassTag[C]].runtimeClass)
      val outCol: Column =
        if (cIsProduct) {
          val wrapped = wrapProductOutput(sqlAgg)
          sqlf.udaf(wrapped).apply(inputCols: _*)("_1")
        } else {
          sqlf.udaf(sqlAgg).apply(inputCols: _*)
        }
      copy(aggregations = GroupedFrame.this.aggregations :+ PlannedAgg.Direct(out.names.head, outCol))
    }

    /** Aggregates using an Algebird `MonoidAggregator[A, B, C]`.
      *
      * If the buffer `B` is SQL-encodable, uses the typed `Aggregator` path. If `B` is Kryo-only (e.g.
      * `Option[SpaceSaver[T]]`), falls back to [[com.sparkling.algebird.MonoidAggregatorUdaf]] with binary buffer serialization; in
      * that case the input must be a single field.
      */
    def apply[A: TypeTag: ClassTag, B, C: TypeTag: ClassTag](
        alg: MonoidAggregator[A, B, C]
    )(implicit
        bEv: EncoderEvidence[B],
        cEv: EncoderEvidence[C],
        bSerDe: BufferSerDe[B]
    ): GroupedFrame = {
      val (in, out) = fs
      require(in.nonEmpty, "aggregate requires at least one input field")
      require(out.nonEmpty, "aggregate requires at least one output field")
      require(out.isSingle, s"aggregate(MonoidAggregator) requires arity * -> 1 output (got ${out.size})")

      bEv.strategy match {
        case _: EncodingStrategy.Kryo =>
          require(in.isSingle, "aggregate(MonoidAggregator) with Kryo buffer requires arity 1 input")
          implicit val cEnc: Encoder[C] = cEv.encoder
          val outCol = algebirdToUdafCol[A, B, C](in.names.head, alg)
          copy(aggregations = GroupedFrame.this.aggregations :+ PlannedAgg.Direct(out.names.head, outCol))

        case _ =>
          apply(algebirdToSql(alg))
      }
    }
  }

  /** Builder for packed (multi-output) typed aggregations created by [[aggregatePacked]].
    *
    * The aggregator result is computed into a temporary column and unpacked into the declared output fields.
    */
  final class PackedAggregateBuilder private[frame] (fs: (Fields, Fields)) {

    /** Packs a raw SQL column expression into a temporary column, then unpacks into `out`. */
    def apply(expr: Column): GroupedFrame = {
      val (_, out) = fs
      require(out.nonEmpty, "aggregatePacked requires at least one output field")
      val tmp = Field.tmp(s"aggPacked_${GroupedFrame.this.aggregations.size}_")
      GroupedFrame.this.addPacked(tmp, out, expr)
    }

    /** Packs a typed Spark `Aggregator[A, B, C]` result into a temporary column, then unpacks into `out`. */
    def apply[A: TypeTag: ClassTag, B, C: ClassTag](
        sqlAgg: SqlAggregator[A, B, C]
    )(implicit
        @unused bEv: EncoderEvidence[B],
        @unused cEv: EncoderEvidence[C]
    ): GroupedFrame = {
      val (in, out) = fs
      require(in.nonEmpty, "aggregatePacked requires at least one input field")
      require(out.nonEmpty, "aggregatePacked requires at least one output field")

      val inputCols = resolveInputCols[A](in)
      val tmp = Field.tmp(s"aggPacked_${GroupedFrame.this.aggregations.size}_")
      val cIsProduct = classOf[Product].isAssignableFrom(implicitly[ClassTag[C]].runtimeClass)
      val packedCol: Column =
        if (cIsProduct) {
          val wrapped = wrapProductOutput(sqlAgg)
          sqlf.udaf(wrapped).apply(inputCols: _*)("_1")
        } else {
          sqlf.udaf(sqlAgg).apply(inputCols: _*)
        }
      GroupedFrame.this.addPacked(tmp, out, packedCol)
    }

    /** Packs an Algebird `MonoidAggregator[A, B, C]` result into a temporary column, then unpacks into `out`. */
    def apply[A: TypeTag: ClassTag, B, C: ClassTag](
        alg: MonoidAggregator[A, B, C]
    )(implicit
        bEv: EncoderEvidence[B],
        cEv: EncoderEvidence[C],
        bSerDe: BufferSerDe[B]
    ): GroupedFrame = {
      val (in, out) = fs
      require(in.nonEmpty, "aggregatePacked requires at least one input field")
      require(out.nonEmpty, "aggregatePacked requires at least one output field")

      bEv.strategy match {
        case _: EncodingStrategy.Kryo =>
          require(in.isSingle, "aggregatePacked(MonoidAggregator) with Kryo buffer requires arity 1 input")
          implicit val cEnc: Encoder[C] = cEv.encoder
          val tmp = Field.tmp(s"aggPacked_${GroupedFrame.this.aggregations.size}_")
          val packedCol = algebirdToUdafCol[A, B, C](in.names.head, alg)
          GroupedFrame.this.addPacked(tmp, out, packedCol)

        case _ =>
          apply(algebirdToSql(alg))
      }
    }
  }

  // ── Private utilities ──────────────────────────────────────────────────────

  /** Expands input columns: if `in` is a single struct column and `A` is a `Product`, splits into sub-field columns. */
  private def resolveInputCols[A: ClassTag](in: Fields): Seq[Column] = {
    val aIsProduct = classOf[Product].isAssignableFrom(implicitly[ClassTag[A]].runtimeClass)
    if (in.isSingle && aIsProduct) {
      frame.df.schema(in.names.head).dataType match {
        case st: StructType => st.fieldNames.toSeq.map(n => sqlf.col(s"${in.names.head}.$n"))
        case _ => Seq(sqlf.col(in.names.head)) // non-struct scalar; pass through, Spark will validate types
      }
    } else {
      in.names.map(sqlf.col)
    }
  }

  /** Wraps `sqlAgg` as `SqlAggregator[A, B, (C, Int)]` to work around Spark nullable top-level product handling. */
  private def wrapProductOutput[A, B, C](
      sqlAgg: SqlAggregator[A, B, C]
  ): SqlAggregator[A, B, (C, Int)] =
    new SqlAggregator[A, B, (C, Int)] {
      override def zero: B = sqlAgg.zero
      override def reduce(b: B, a: A): B = sqlAgg.reduce(b, a)
      override def merge(b1: B, b2: B): B = sqlAgg.merge(b1, b2)
      override def finish(b: B): (C, Int) = (sqlAgg.finish(b), 0)
      override def bufferEncoder: Encoder[B] = sqlAgg.bufferEncoder
      override def outputEncoder: Encoder[(C, Int)] = Encoders.tuple(sqlAgg.outputEncoder, Encoders.scalaInt)
    }
}
