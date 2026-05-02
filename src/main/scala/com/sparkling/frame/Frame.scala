package com.sparkling.frame

import scala.collection.immutable.ArraySeq

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{functions => sqlf, Column, DataFrame, Encoder}
import org.apache.spark.storage.StorageLevel

import com.sparkling.schema.{Field, Fields}

/** Fluent, DataFrame-first transformation API.
  *
  * A thin DSL wrapper around Spark `DataFrame` that favors explicit, composable transformations using
  * [[com.sparkling.schema.Fields]] for column references.
  *
  * Notes:
  *   - Methods never mutate the underlying DataFrame; each returns a new [[Frame]].
  *   - Mapping-like operations (e.g. rename, alias) require equal input/output arity.
  *
  * @param df underlying Spark DataFrame being transformed
  */
final case class Frame(df: DataFrame) {

  /** Returns the underlying Spark schema. */
  def schema: StructType = df.schema

  /** Returns the underlying column names in order. */
  def columns: Seq[String] = df.columns.toSeq

  /** Returns a column reference from the underlying DataFrame.
    *
    * @param name column name
    */
  def col(name: String): Column = df.col(name)

  /** Returns true iff all requested fields exist as columns in this Frame.
    *
    * @param fs fields that must exist as columns
    */
  def hasFields(fs: Fields): Boolean = {
    val set = df.columns.toSet
    fs.names.forall(set.contains)
  }

  /** Returns true if this Frame has zero rows.
    *
    * @note Triggers a Spark action (`take(1)`). Avoid calling in tight loops or more than once per logical check.
    */
  def isEmpty: Boolean = df.head(1).isEmpty

  /** Returns true if this Frame has at least one row.
    *
    * @note Triggers a Spark action (`take(1)`). Avoid calling in tight loops or more than once per logical check.
    */
  def nonEmpty: Boolean = !isEmpty

  /** Applies `f` to this Frame and returns the result.
    *
    * Useful for expression-oriented composition when you want to apply an arbitrary function to the current value
    * without breaking a method chain.
    *
    * @param f function applied to this [[Frame]]
    * @tparam T result type
    */
  def thenDo[T](f: Frame => T): T = f(this)

  /** Conditionally applies an operation if the optional value is present.
    *
    * Example:
    * {{{
    * frame.maybeDo(maybeLimit) { (fr, n) => fr.limit(n) }
    * }}}
    *
    * @param x optional value that controls whether `f` is applied
    * @param f function applied when `x` is defined
    * @tparam T optional value type
    */
  def maybeDo[T](x: Option[T])(f: (Frame, T) => Frame): Frame =
    x.map(t => f(this, t)).getOrElse(this)

  /** Conditionally applies an operation if `cond` is true.
    *
    * Example:
    * {{{
    * frame.maybeDo(debug)(_.printSchemaAndReturn())
    * }}}
    *
    * @param cond condition controlling whether `f` is applied
    * @param f transformation applied when `cond` is true
    */
  def maybeDo(cond: Boolean)(f: Frame => Frame): Frame =
    if (cond) f(this) else this

  /** Returns the number of rows. */
  def count(): Long = df.count()

  /** Prints the schema to stdout (side effect). */
  def printSchema(): Unit = df.printSchema()

  /** Prints the schema and returns this Frame for chaining (debug/tap helper). */
  def printSchemaAndReturn(): Frame = {
    df.printSchema()
    this
  }

  /** Shows rows (side effect).
    *
    * @param numRows number of rows to show
    * @param truncate whether to truncate long strings
    */
  def show(numRows: Int = 20, truncate: Boolean = true): Unit =
    df.show(numRows, truncate)

  /** Shows rows and returns this Frame for chaining (debug/tap helper).
    *
    * @param numRows number of rows to show
    * @param truncate whether to truncate long strings
    */
  def showAndReturn(numRows: Int = 20, truncate: Boolean = true): Frame = {
    df.show(numRows, truncate)
    this
  }

  /** Explains the query plan (side effect).
    *
    * @param extended whether to print the extended plan
    */
  def explain(extended: Boolean = false): Unit =
    df.explain(extended)

  /** Returns a cached version of this Frame (lazy until an action is run). */
  def cache(): Frame = Frame(df.cache())

  /** Returns a persisted version of this Frame using the given storage level (lazy until an action is run).
    *
    * To force immediate materialization, chain with [[materialize]]:
    * {{{
    * frame.persist(StorageLevel.MEMORY_ONLY).materialize()
    * }}}
    *
    * @param level Spark storage level to persist with
    */
  def persist(level: StorageLevel): Frame = Frame(df.persist(level))

  /** Returns an unpersisted version of this Frame.
    *
    * @param blocking whether to block until unpersist completes
    */
  def unpersist(blocking: Boolean = false): Frame = Frame(df.unpersist(blocking))

  /** Materializes this Frame by triggering an action and returns this Frame for chaining.
    *
    * This is useful after [[cache]] / [[persist]] when you want to force evaluation immediately.
    */
  def materialize(): Frame = {
    df.count()
    this
  }

  /** Limits to the first `n` rows.
    *
    * @param n maximum number of rows
    */
  def limit(n: Int): Frame = Frame(df.limit(n))

  /** Takes up to `num` rows after projecting the requested fields and decoding them as `T`.
    *
    * @param fs fields to project before decoding (must be non-empty)
    * @param num maximum number of rows to take
    * @tparam T target element type (must have an implicit Spark `Encoder`)
    * @throws java.lang.IllegalArgumentException if `fs` is empty
    */
  def take[T: Encoder](fs: Fields, num: Int): IndexedSeq[T] = {
    require(fs.nonEmpty, "take requires at least one field")
    ArraySeq.unsafeWrapArray(df.select(fs.names.map(sqlf.col): _*).as[T].take(num))
  }

  /** Repartitions to a fixed number of partitions.
    *
    * @param numPartitions target number of partitions
    */
  def repartition(numPartitions: Int): Frame =
    Frame(df.repartition(numPartitions))

  /** Repartitions by key columns.
    *
    * @param fs key columns
    */
  def repartition(fs: Fields): Frame =
    Frame(df.repartition(fs.names.map(sqlf.col): _*))

  /** Repartitions to a fixed number of partitions, using key columns for distribution.
    *
    * @param numPartitions target number of partitions
    * @param fs key columns
    */
  def repartition(numPartitions: Int, fs: Fields): Frame =
    Frame(df.repartition(numPartitions, fs.names.map(sqlf.col): _*))

  /** Coalesces to a smaller number of partitions (no shuffle).
    *
    * @param numPartitions target number of partitions
    */
  def coalesce(numPartitions: Int): Frame =
    Frame(df.coalesce(numPartitions))

  /** Sorts within partitions only (no global shuffle beyond existing partitioning).
    *
    * @param fs columns to sort by (must be non-empty)
    * @param descending if true, sorts descending; ascending by default
    * @throws java.lang.IllegalArgumentException if `fs` is empty
    */
  def sortWithinPartitions(fs: Fields, descending: Boolean = false): Frame = {
    require(fs.nonEmpty, "sortWithinPartitions requires at least one field")
    val cols =
      if (!descending) fs.names.map(sqlf.col)
      else fs.names.map(n => sqlf.col(n).desc)
    Frame(df.sortWithinPartitions(cols: _*))
  }

  /** Orders globally by the given columns (ascending by default).
    *
    * @param fs columns to sort by (must be non-empty)
    * @param descending if true, sorts descending; ascending by default
    * @throws java.lang.IllegalArgumentException if `fs` is empty
    */
  def orderBy(fs: Fields, descending: Boolean = false): Frame = {
    require(fs.nonEmpty, "orderBy requires at least one field")
    val cols =
      if (!descending) fs.names.map(sqlf.col)
      else fs.names.map(n => sqlf.col(n).desc)
    Frame(df.orderBy(cols: _*))
  }

  /** Orders globally by the given columns with explicit per-column directions.
    *
    * @param fs sequence of (field, descending) pairs (must be non-empty)
    * @throws java.lang.IllegalArgumentException if `fs` is empty
    */
  def orderBy(fs: Seq[(Field, Boolean)]): Frame = {
    require(fs.nonEmpty, "orderBy requires at least one field")
    val cols = fs.map { case (f, desc) => if (desc) sqlf.col(f.name).desc else sqlf.col(f.name) }
    Frame(df.orderBy(cols: _*))
  }
}
