package com.sparkling.frame

import scala.collection.immutable.ArraySeq

import org.apache.spark.sql.types.{DataType, StructType}
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

  /** Keeps only the specified columns, preserving their order.
    *
    * @param fs columns to keep (in the desired output order)
    */
  def project(fs: Fields): Frame =
    Frame(df.select(fs.names.map(sqlf.col): _*))

  /** Drops the specified columns.
    *
    * @param fs columns to drop
    */
  def discard(fs: Fields): Frame =
    Frame(df.drop(fs.names: _*))

  /** Renames one or more columns.
    *
    * Examples:
    * {{{
    * frame.rename("some_field" -> "someField")
    * frame.rename(("a", "b") -> ("x", "y"))
    * }}}
    *
    * @param fs mapping from existing column names to new column names (must have equal arity)
    * @throws java.lang.IllegalArgumentException if the mapping arity differs
    */
  def rename(fs: (Fields, Fields)): Frame = {
    val (from, to) = fs
    require(from.size == to.size, s"rename requires same arity (from=${from.size}, to=${to.size})")

    val renamed =
      from.names.zip(to.names).foldLeft(df) { case (acc, (src, dst)) =>
        acc.withColumnRenamed(src, dst)
      }

    Frame(renamed)
  }

  /** Copies one or more columns under new names, keeping the originals.
    *
    * Note: unlike a SQL alias (which renames in place), this adds new columns while preserving the source columns.
    * To rename without keeping the original, use [[rename]].
    *
    * Examples:
    * {{{
    * frame.alias("someField" -> "someFieldAlias")
    * frame.alias(("a", "b") -> ("x", "y"))
    * }}}
    *
    * @param fs mapping from source columns to new alias columns (must have equal arity)
    * @throws java.lang.IllegalArgumentException if the mapping arity differs
    */
  def alias(fs: (Fields, Fields)): Frame = {
    val (from, to) = fs
    require(from.size == to.size, s"alias requires same arity (from=${from.size}, to=${to.size})")

    val withAliases =
      from.names.zip(to.names).foldLeft(df) { case (acc, (src, dst)) =>
        acc.withColumn(dst, sqlf.col(src))
      }

    Frame(withAliases)
  }

  /** Packs multiple input fields into a single struct column.
    *
    * The struct field names match the input column names.
    *
    * @param fs mapping from input fields (must be non-empty) to a single output field
    * @throws java.lang.IllegalArgumentException if the input fields are empty
    */
  def pack(fs: (Fields, Field)): Frame = {
    val (in, out) = fs
    require(in.nonEmpty, "pack requires at least one input field")

    val structExpr = sqlf.struct(in.names.map(n => sqlf.col(n).as(n)): _*)
    Frame(df.withColumn(out.name, structExpr))
  }

  /** Unpacks a nested struct or array column into multiple top-level columns, dropping the input column.
    *
    * Behavior depends on the input column type:
    *   - `StructType`: fields are extracted by name (schema order) and renamed to the provided output names; output
    *     arity must match the struct arity exactly.
    *   - `ArrayType`: elements are extracted by index.
    *
    * @param fs mapping from an input field to output fields (output must be non-empty)
    * @throws java.lang.IllegalArgumentException if output fields are empty, arity mismatches, or the input column is
    *   not a `StructType` or `ArrayType`
    */
  def unpack(fs: (Field, Fields)): Frame = {
    val (in, out) = fs
    require(out.nonEmpty, "unpack requires at least one output field")

    val dt = df.schema(in.name).dataType
    val inCol = sqlf.col(in.name)

    val unpackedExprs: IndexedSeq[Column] = dt match {
      case StructType(structFields) =>
        require(
          out.size == structFields.length,
          s"provided ${out.size} output fields but struct has ${structFields.length} fields"
        )
        structFields.indices.map(i => inCol.getField(structFields(i).name).as(out.names(i)))

      case org.apache.spark.sql.types.ArrayType(_, _) =>
        out.names.indices.map(i => inCol.getItem(i).as(out.names(i)))

      case other =>
        throw new IllegalArgumentException(s"can only unpack a StructType or ArrayType, not $other")
    }

    val withOut =
      out.names
        .zip(unpackedExprs)
        .foldLeft(df) { case (acc, (name, expr)) => acc.withColumn(name, expr) }

    Frame(withOut.drop(in.name))
  }

  /** Unpacks a struct column into top-level fields using the struct's own field names, dropping the input column.
    *
    * This is a convenience overload for the common case where you do not need to rename the unpacked fields.
    *
    * @param in struct field to unpack
    * @throws java.lang.IllegalArgumentException if `in` is not a `StructType`
    */
  def unpack(in: Field): Frame = {
    val dt = df.schema(in.name).dataType
    dt match {
      case StructType(structFields) =>
        val out = Fields.fromSeq(structFields.iterator.map(_.name).toSeq)
        unpack(in -> out)
      case other =>
        throw new IllegalArgumentException(s"unpack(Field) requires a StructType column, not $other")
    }
  }

  /** Writes a constant literal value into one or more output columns.
    *
    * If an output column already exists it is overwritten; otherwise it is appended.
    *
    * @param out output columns to write (must be non-empty)
    * @param value literal value to write into each output column
    * @tparam T value type
    * @throws java.lang.IllegalArgumentException if `out` is empty
    */
  def insert[T](out: Fields, value: T): Frame = {
    require(out.nonEmpty, "insert requires at least one output field")
    val literal = sqlf.lit(value)
    val resultDf = out.names.foldLeft(df)((acc, name) => acc.withColumn(name, literal))
    Frame(resultDf)
  }

  /** Casts one or more columns to the given Spark `DataType`, writing results to output columns.
    *
    * Input and output field sets must have equal arity. To cast a column in place, use the single-field overload.
    *
    * @param fs mapping from input columns to output columns (both sides must be non-empty and equal in size)
    * @param dataType target Spark SQL `DataType`
    * @throws java.lang.IllegalArgumentException if either field set is empty or arities differ
    */
  def cast(fs: (Fields, Fields), dataType: DataType): Frame = {
    val (in, out) = fs
    require(in.nonEmpty, "cast requires at least one input field")
    require(out.nonEmpty, "cast requires at least one output field")
    require(in.size == out.size, s"cast requires same arity (in=${in.size}, out=${out.size})")

    val outDf =
      in.names.zip(out.names).foldLeft(df) { case (acc, (src, dst)) =>
        acc.withColumn(dst, acc.col(src).cast(dataType))
      }

    Frame(outDf)
  }

  /** Casts a single column to the given Spark `DataType` in place.
    *
    * @param field column to cast
    * @param dataType target Spark SQL `DataType`
    */
  def cast(field: Field, dataType: DataType): Frame =
    cast(Fields.one(field) -> Fields.one(field), dataType)

  /** Adds or replaces a column using a Spark SQL expression.
    *
    * @param field output column to write
    * @param expr expression to write into `field`
    */
  def withColumn(field: Field, expr: Column): Frame =
    Frame(df.withColumn(field.name, expr))

  /** Adds or replaces columns using a single Spark SQL expression.
    *
    * If `fields.size == 1`, the expression may return any Spark-supported type and is written directly.
    *
    * If `fields.size > 1`, the expression must evaluate to a Spark struct (e.g., a tuple or case class); the struct's
    * fields are expanded into the output columns by position. If the struct contains more fields than `fields`, the
    * extra struct fields are ignored.
    *
    * @param fields output columns to write (must be non-empty)
    * @param expr expression to write; if `fields.size > 1` it must be a struct with at least `fields.size` fields
    * @throws java.lang.IllegalArgumentException if `fields` is empty or if multi-output expansion fails
    */
  def withColumns(fields: Fields, expr: Column): Frame =
    assignColumns(fields, expr)

  /** Adds or replaces columns using a sequence of Spark SQL expressions.
    *
    * Each expression is written to the corresponding output field by position. The number of expressions must match
    * `fields.size`.
    *
    * @param fields output columns to write (must be non-empty)
    * @param cols expressions to write; must have the same size as `fields`
    * @throws java.lang.IllegalArgumentException if `fields` is empty or if `cols.size != fields.size`
    */
  def withColumns(fields: Fields, cols: Seq[Column]): Frame =
    assignColumns(fields, cols)

  /** Writes a single expression into one or more output columns.
    *
    * For single output, writes the expression directly. For multiple outputs, the expression must return a struct;
    * the struct fields are expanded into the output columns by position.
    */
  private[frame] def assignColumns(out: Fields, expr: Column): Frame = {
    require(out.nonEmpty, "output fields must be non-empty")

    val resultDf =
      if (out.isSingle) {
        df.withColumn(out.names.head, expr)
      } else {
        val tmp = Field.tmp(prefix = "__tmp_")

        val withTmp = df.withColumn(tmp.name, expr)

        val st = withTmp.schema(tmp.name).dataType match {
          case s: StructType => s
          case other         =>
            throw new IllegalArgumentException(
              s"cannot write ${out.size} output columns because expression did not return a struct. " +
                s"Return a tuple/case class (struct) or use a single output column. Got: $other"
            )
        }

        val structArity = st.fields.length
        if (structArity < out.size) {
          val want = out.names.mkString(", ")
          val got = st.fieldNames.mkString(", ")
          throw new IllegalArgumentException(
            s"output arity mismatch: requested ${out.size} columns ($want) but expression returned $structArity fields ($got)"
          )
        }

        val tmpCol = sqlf.col(tmp.name)
        val expanded =
          out.names.zipWithIndex.foldLeft(withTmp) { case (acc, (name, i)) =>
            acc.withColumn(name, tmpCol.getField(st.fieldNames(i)))
          }

        expanded.drop(tmp.name)
      }

    Frame(resultDf)
  }

  /** Writes a sequence of expressions into output columns by position. */
  private[frame] def assignColumns(out: Fields, cols: Seq[Column]): Frame = {
    require(out.nonEmpty, "output fields must be non-empty")
    require(
      cols.size == out.size,
      s"output arity mismatch: requested ${out.size} columns but got ${cols.size} expressions"
    )

    val resultDf = out.names
      .zip(cols)
      .foldLeft(df) { case (acc, (name, c)) => acc.withColumn(name, c) }

    Frame(resultDf)
  }

  // ── Cleaning ────────────────────────────────────────────────────────────────

  /** Replaces null values in the given columns with a `Double` fill value.
    *
    * @param fs columns to fill (must be non-empty)
    * @param value replacement value for nulls
    * @throws java.lang.IllegalArgumentException if `fs` is empty
    */
  def fillNulls(fs: Fields, value: Double): Frame = {
    require(fs.nonEmpty, "fillNulls requires at least one field")
    Frame(df.na.fill(value, fs.names))
  }

  /** Replaces null values in the given columns with a `Long` fill value.
    *
    * @param fs columns to fill (must be non-empty)
    * @param value replacement value for nulls
    * @throws java.lang.IllegalArgumentException if `fs` is empty
    */
  def fillNulls(fs: Fields, value: Long): Frame = {
    require(fs.nonEmpty, "fillNulls requires at least one field")
    Frame(df.na.fill(value, fs.names))
  }

  /** Replaces null values in the given columns with a `Boolean` fill value.
    *
    * @param fs columns to fill (must be non-empty)
    * @param value replacement value for nulls
    * @throws java.lang.IllegalArgumentException if `fs` is empty
    */
  def fillNulls(fs: Fields, value: Boolean): Frame = {
    require(fs.nonEmpty, "fillNulls requires at least one field")
    Frame(df.na.fill(value, fs.names))
  }

  /** Replaces null values in the given columns with a `String` fill value.
    *
    * @param fs columns to fill (must be non-empty)
    * @param value replacement value for nulls
    * @throws java.lang.IllegalArgumentException if `fs` is empty
    */
  def fillNulls(fs: Fields, value: String): Frame = {
    require(fs.nonEmpty, "fillNulls requires at least one field")
    Frame(df.na.fill(value, fs.names))
  }

  /** Replaces string values in the given columns according to a substitution map.
    *
    * @param fs columns to apply replacements to (must be non-empty)
    * @param replacements map of existing string values to their replacements
    * @throws java.lang.IllegalArgumentException if `fs` is empty
    */
  def replace(fs: Fields, replacements: Map[String, String]): Frame = {
    require(fs.nonEmpty, "replace requires at least one field")
    Frame(df.na.replace(fs.names, replacements))
  }

  /** Returns a Frame with duplicate rows removed across all columns. */
  def distinct(): Frame = Frame(df.distinct())

  /** Returns a Frame with rows deduplicated on the given columns.
    *
    * Equivalent to SQL `SELECT DISTINCT ON (cols)`. When multiple rows share the same values in `fs`, one is kept
    * and the rest are dropped.
    *
    * @param fs columns to deduplicate on (must be non-empty)
    */
  def distinct(fs: Fields): Frame = Frame(df.dropDuplicates(fs.names))

  /** Drops rows where any of the given columns is null.
    *
    * @param fs columns to check for nulls (must be non-empty)
    * @throws java.lang.IllegalArgumentException if `fs` is empty
    */
  def dropNulls(fs: Fields): Frame = {
    require(fs.nonEmpty, "dropNulls requires at least one field")
    Frame(df.na.drop("any", fs.names))
  }

  /** Drops rows where all of the given columns are null.
    *
    * A row is dropped only if every column in `fs` is null. Rows with at least one non-null value in `fs` are kept.
    *
    * @param fs columns to check for nulls (must be non-empty)
    * @throws java.lang.IllegalArgumentException if `fs` is empty
    */
  def dropAllNulls(fs: Fields): Frame = {
    require(fs.nonEmpty, "dropAllNulls requires at least one field")
    Frame(df.na.drop("all", fs.names))
  }

  /** Drops rows where any of the given columns is null. Alias for [[dropNulls]].
    *
    * @param fs columns to check for nulls (must be non-empty)
    * @throws java.lang.IllegalArgumentException if `fs` is empty
    */
  def filterNotNull(fs: Fields): Frame = dropNulls(fs)

  /** Filters rows by a Spark SQL column predicate.
    *
    * @param pred boolean column expression to filter by
    */
  def where(pred: Column): Frame = Frame(df.filter(pred))

  /** Returns a random sample of rows.
    *
    * @param fraction fraction of rows to include; must be non-negative; must be in `[0.0, 1.0]` when sampling without
    *   replacement; may exceed `1.0` when sampling with replacement (expected multiplier)
    * @param withReplacement whether to sample with replacement (default false)
    * @param seed optional random seed for reproducibility
    * @throws java.lang.IllegalArgumentException if `fraction` is negative, or exceeds `1.0` without replacement
    */
  def sample(fraction: Double, withReplacement: Boolean = false, seed: Option[Long] = None): Frame = {
    require(fraction >= 0.0, s"sample fraction must be non-negative (got $fraction)")
    require(withReplacement || fraction <= 1.0, s"sample fraction must be in [0.0, 1.0] (got $fraction)")
    val out = seed match {
      case Some(s) => df.sample(withReplacement = withReplacement, fraction = fraction, seed = s)
      case None    => df.sample(withReplacement = withReplacement, fraction = fraction)
    }
    Frame(out)
  }
}
