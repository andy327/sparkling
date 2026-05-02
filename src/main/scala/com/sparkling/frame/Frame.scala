package com.sparkling.frame

import scala.collection.immutable.ArraySeq

import org.apache.spark.sql.types.{ArrayType, DataType, MapType, StructType}
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

  // ── Cleaning ───────────────────────────────────────────────────────────────

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

  /** Replaces null values in the given columns with an `Int` fill value.
    *
    * @param fs columns to fill (must be non-empty)
    * @param value replacement value for nulls
    * @throws java.lang.IllegalArgumentException if `fs` is empty
    */
  def fillNulls(fs: Fields, value: Int): Frame = fillNulls(fs, value.toLong)

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
    * @throws java.lang.IllegalArgumentException if `fs` is empty
    */
  def distinct(fs: Fields): Frame = {
    require(fs.nonEmpty, "distinct requires at least one field")
    Frame(df.dropDuplicates(fs.names))
  }

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

  // ── Transforms ─────────────────────────────────────────────────────────────

  /** Explodes an array column into one row per element, writing results to one or more output columns.
    *
    * If `out` is a single field, the exploded element is written directly to that column. If `out` has multiple
    * fields, the array elements must be structs; each struct's fields are unpacked by position into the output columns
    * (equivalent to calling `unpack` after exploding).
    *
    * The input column is dropped from the output.
    *
    * @param fs mapping from the array input column to one or more output columns (output must be non-empty)
    * @param outer if true, rows with a null or empty array produce one output row with null values (SQL
    *   `EXPLODE_OUTER`); if false, such rows are dropped (default)
    * @throws java.lang.IllegalArgumentException if output fields are empty
    */
  def flatten(fs: (Field, Fields), outer: Boolean = false): Frame = {
    val (in, out) = fs
    require(out.nonEmpty, "flatten requires at least one output field")

    val tmp = Field.tmp(prefix = s"${in.name}_elem_")
    val explodeFn = if (outer) sqlf.explode_outer _ else sqlf.explode _
    // Explode into tmp and drop the input column in one step; keeps the plan clean.
    val exploded = df.withColumn(tmp.name, explodeFn(sqlf.col(in.name))).drop(in.name)

    val outDf =
      if (out.isSingle)
        exploded.withColumnRenamed(tmp.name, out.names.head)
      else
        Frame(exploded).unpack(tmp -> out).df

    Frame(outDf)
  }

  /** Rotates multiple input columns into rows of (key, value) pairs.
    *
    * Each input column name becomes a value of `keyField` and the column's value becomes the value of `valueField`.
    * All other columns are preserved unchanged. Input columns are dropped from the output.
    *
    * Example: unpivoting `("a", "b")` from `(id=1, a=10, b=20)` with `key="col"` and `value="val"` yields:
    * {{{
    * (id=1, col="a", val=10)
    * (id=1, col="b", val=20)
    * }}}
    *
    * @param in input columns to unpivot (must be non-empty and must exist in this Frame)
    * @param keyField output column that receives the source column name
    * @param valueField output column that receives the source column value
    * @throws java.lang.IllegalArgumentException if `in` is empty or any field is missing
    */
  def unpivot(in: Fields, keyField: Field, valueField: Field): Frame = {
    require(in.nonEmpty, "unpivot requires at least one input field")
    require(hasFields(in), s"unpivot input fields must exist: ${in.names.mkString(", ")}")

    val tmp = Field.tmp(prefix = "__unpivot_")
    val keepCols = df.columns.toSeq.filterNot(in.containsName).map(sqlf.col)
    val pairs = in.names.map { n =>
      sqlf.struct(sqlf.lit(n).as(keyField.name), sqlf.col(n).as(valueField.name))
    }
    val exploded =
      df.select((keepCols :+ sqlf.explode(sqlf.array(pairs: _*)).as(tmp.name)): _*)

    Frame(exploded).unpack(tmp -> Fields(keyField.name, valueField.name))
  }

  /** Recursively unpivots nested columns (structs, arrays, maps) into a flat sequence of (path, value) pairs.
    *
    * Traverses each input column's schema recursively. Terminal (scalar) values produce one row per leaf with a
    * dot-separated path as the key (e.g. `"address.city"`) and the value cast to `String`. Null containers produce
    * one row with the container's path and a null value. All other columns are preserved.
    *
    * @param in input columns to recursively unpivot (must be non-empty and must exist in this Frame)
    * @param keyField output column that receives the dot-separated path to the leaf
    * @param valueField output column that receives the leaf value cast to `String`
    * @throws java.lang.IllegalArgumentException if `in` is empty or any field is missing
    */
  def deepUnpivot(in: Fields, keyField: Field, valueField: Field): Frame = {
    require(in.nonEmpty, "deepUnpivot requires at least one input field")
    require(hasFields(in), s"deepUnpivot input fields must exist: ${in.names.mkString(", ")}")

    val tmp = Field.tmp(prefix = "__deepUnpivot_")
    val keepCols = df.columns.toSeq.filterNot(in.containsName).map(sqlf.col)

    def mkPair(path: Column, value: Column): Column =
      sqlf.struct(path.as(keyField.name), value.cast("string").as(valueField.name))

    def traverse(dt: DataType, c: Column, path: Column): Column = {
      val nonNull: Column = dt match {
        case StructType(fields) =>
          val children = fields.map { f =>
            val childPath =
              sqlf.when(path.isNull, sqlf.lit(f.name))
                .otherwise(sqlf.concat_ws(".", path, sqlf.lit(f.name)))
            traverse(f.dataType, c.getField(f.name), childPath)
          }
          sqlf.flatten(sqlf.array(children.toIndexedSeq: _*))

        case ArrayType(elemType, _) =>
          sqlf.flatten(sqlf.transform(c, elem => traverse(elemType, elem, path)))

        case MapType(_, valueType, _) =>
          sqlf.flatten(
            sqlf.transform(
              sqlf.map_entries(c),
              entry => {
                val keyPath =
                  sqlf.when(path.isNull, entry.getField("key").cast("string"))
                    .otherwise(sqlf.concat_ws(".", path, entry.getField("key").cast("string")))
                traverse(valueType, entry.getField("value"), keyPath)
              }
            )
          )

        case _ =>
          sqlf.array(mkPair(path, c))
      }
      sqlf.when(c.isNull, sqlf.array(mkPair(path, sqlf.lit(null)))).otherwise(nonNull)
    }

    val rootPairs = in.names.map { name =>
      val dt = df.schema(name).dataType
      traverse(dt, sqlf.col(name), sqlf.lit(name))
    }

    val exploded =
      df.select(
        (keepCols :+
          sqlf.explode(sqlf.flatten(sqlf.array(rootPairs: _*))).as(tmp.name)): _*
      )

    Frame(exploded).unpack(tmp -> Fields(keyField.name, valueField.name))
  }

  /** Adds a column of monotonically increasing 64-bit IDs.
    *
    * IDs are unique and non-negative within a single job but are not consecutive, are not stable across re-runs, and
    * should not be used as durable surrogate keys.
    *
    * @param out output column to write the ID into
    */
  def uniqueId(out: Field): Frame =
    Frame(df.withColumn(out.name, sqlf.monotonically_increasing_id()))

  /** Adds a column of randomly generated UUIDs (version 4).
    *
    * Each row receives a distinct UUID string of the form `xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx`.
    *
    * @param out output column to write the UUID into
    */
  def uuid(out: Field): Frame =
    Frame(df.withColumn(out.name, sqlf.uuid()))

  /** Hashes the given input columns into a single 64-bit output column using `xxhash64`.
    *
    * @param fs mapping from input columns (must be non-empty) to the output column
    * @throws java.lang.IllegalArgumentException if input fields are empty
    */
  def hash(fs: (Fields, Field)): Frame = {
    val (in, out) = fs
    require(in.nonEmpty, "hash requires at least one input field")
    Frame(df.withColumn(out.name, sqlf.xxhash64(in.names.map(sqlf.col): _*)))
  }

  /** Hashes all columns in schema order into a single 64-bit output column using `xxhash64`.
    *
    * @note The hash depends on the current schema column order. Reordering or adding columns changes the hash values.
    * @param out output column to write the hash into
    */
  def hash(out: Field): Frame = {
    val cols = ArraySeq.unsafeWrapArray(df.columns.map(sqlf.col))
    Frame(df.withColumn(out.name, sqlf.xxhash64(cols: _*)))
  }

  /** Returns the first non-null value across the given input columns, written to an output column.
    *
    * Equivalent to SQL `COALESCE(col1, col2, ...)`. Input columns are evaluated left to right; the first non-null
    * value is returned. If all inputs are null, the output is null.
    *
    * @note Not to be confused with [[coalesce(numPartitions:Int)*]], which reduces the number of partitions.
    * @param fs mapping from input columns (must be non-empty) to the output column
    * @throws java.lang.IllegalArgumentException if input fields are empty
    */
  def firstNonNull(fs: (Fields, Field)): Frame = {
    val (in, out) = fs
    require(in.nonEmpty, "firstNonNull requires at least one input field")
    Frame(df.withColumn(out.name, sqlf.coalesce(in.names.map(sqlf.col): _*)))
  }

  // ── Relational ─────────────────────────────────────────────────────────────

  /** Hints to Spark that this Frame is small enough to broadcast in a join.
    *
    * Equivalent to wrapping the underlying DataFrame with `sqlf.broadcast`. Useful when joining a small lookup table
    * against a large one to avoid a shuffle.
    */
  def broadcast(): Frame = Frame(sqlf.broadcast(df))

  /** Joins this Frame against `other` on explicitly named key columns.
    *
    * Left and right key fields are matched by position; both sides must have equal arity. Column names that exist on
    * both sides after the join are disambiguated automatically by Spark (accessible via `frame.col("name")`).
    *
    * @param on mapping from left key columns to right key columns (must be non-empty and equal in size)
    * @param other Frame to join against
    * @param how join strategy (default [[JoinType.Inner]])
    * @throws java.lang.IllegalArgumentException if key arities differ or no keys are provided
    */
  def join(on: (Fields, Fields), other: Frame, how: JoinType = JoinType.Inner): Frame = {
    val (leftKeys, rightKeys) = on
    require(
      leftKeys.size == rightKeys.size,
      s"join requires same arity (left=${leftKeys.size}, right=${rightKeys.size})"
    )
    val cond =
      leftKeys.names
        .zip(rightKeys.names)
        .map { case (l, r) => df(l) === other.df(r) }
        .reduceOption(_ && _)
        .getOrElse(throw new IllegalArgumentException("join requires at least one key field"))
    Frame(df.join(other.df, cond, how.sparkName))
  }

  /** Joins this Frame against `other` on shared key columns (same name on both sides).
    *
    * A convenience overload for the common case where the join keys have the same name in both frames. The shared key
    * column appears exactly once in the result (Spark deduplicates it automatically). For asymmetric key names use
    * the `(Fields, Fields)` overload, where both copies are retained.
    *
    * @param on key columns present (by the same name) in both frames (must be non-empty)
    * @param other Frame to join against
    * @param how join strategy (default [[JoinType.Inner]])
    * @throws java.lang.IllegalArgumentException if `on` is empty
    */
  def join(on: Fields, other: Frame, how: JoinType): Frame =
    Frame(df.join(other.df, on.names, how.sparkName))

  /** Joins this Frame against `other` on shared key columns using an inner join. */
  def join(on: Fields, other: Frame): Frame =
    Frame(df.join(other.df, on.names, JoinType.Inner.sparkName))

  /** Stacks this Frame on top of `other`, aligning columns by name and padding missing columns with nulls.
    *
    * Equivalent to `Frame.merge(Seq(this, other))`.
    *
    * @param other Frame to union with
    */
  def ++(other: Frame): Frame = Frame.merge(Seq(this, other))

  // ── Grouped ─────────────────────────────────────────────────────────────────

  /** Groups by the given key columns, applies aggregations defined by `f`, and returns the aggregated Frame.
    *
    * Example:
    * {{{
    * frame.groupBy(Fields("dept")) {
    *   _.count(Field("n"))
    *    .avg(Fields("salary") -> Field("avg_salary"))
    * }
    * }}}
    *
    * @param keys grouping key columns (must be non-empty)
    * @param f function that builds aggregations on a [[GroupedFrame]]
    * @throws java.lang.IllegalArgumentException if `keys` is empty or no aggregations are added
    */
  def groupBy(keys: Fields)(f: GroupedFrame => GroupedFrame): Frame = {
    require(keys.nonEmpty, "groupBy requires at least one key field")
    val gf = GroupedFrame(this, keys, Vector.empty, Vector.empty, None)
    f(gf).run()
  }

  /** Aggregates all rows into a single row, applying aggregations defined by `f`.
    *
    * Equivalent to a SQL `SELECT agg(...) FROM table` with no `GROUP BY` clause.
    *
    * @param f function that builds aggregations on a [[GroupedFrame]]
    * @throws java.lang.IllegalArgumentException if no aggregations are added
    */
  def groupAll(f: GroupedFrame => GroupedFrame): Frame = {
    val gf = GroupedFrame(this, Fields.empty, Vector.empty, Vector.empty, None)
    f(gf).run()
  }
}

object Frame {

  /** Combines multiple Frames by stacking their rows, aligning columns by name.
    *
    * Three behaviors depending on column overlap:
    *   - All frames share the exact same schema: plain `UNION ALL` by name.
    *   - `keepAll = true` (default): extra columns from any frame are kept; frames missing a column have it padded
    *     with nulls.
    *   - `keepAll = false`: only columns common to every frame are retained; extra columns are dropped.
    *
    * @param frames frames to merge (must be non-empty)
    * @param keepAll if true, retain all columns across all frames, padding absent ones with null; if false, project to
    *   the common schema only (default true)
    * @throws java.lang.IllegalArgumentException if `frames` is empty or no common columns exist
    */
  def merge(frames: Seq[Frame], keepAll: Boolean = true): Frame = {
    require(frames.nonEmpty, "merge requires at least one input")

    if (frames.size == 1) frames.head
    else {
      val dataFrames = frames.map(_.df)
      val commonColumnSet = dataFrames.iterator.map(_.columns.toSet).reduce(_ intersect _)
      val commonColumns = dataFrames.head.columns.filter(commonColumnSet).toSeq
      require(commonColumns.nonEmpty, "merge requires at least one column in common across all frames")

      val allHaveOnlyCommon = dataFrames.forall(_.columns.toSet == commonColumnSet)

      if (allHaveOnlyCommon) {
        Frame(dataFrames.reduce(_ unionByName _))
      } else if (keepAll) {
        val extraColumns =
          dataFrames.iterator
            .flatMap(_.columns.iterator.filterNot(commonColumnSet))
            .toVector
            .distinct

        val aligned = dataFrames.map { df =>
          val present = df.columns.toSet
          val commonExprs = commonColumns.map(df.col)
          val extraExprs = extraColumns.map { name =>
            if (present.contains(name)) df.col(name)
            else sqlf.lit(null).as(name)
          }
          df.select((commonExprs ++ extraExprs): _*)
        }
        Frame(aligned.reduce(_ unionByName _))
      } else {
        val projected = dataFrames.map(_.select(commonColumns.map(sqlf.col): _*))
        Frame(projected.reduce(_ unionByName _))
      }
    }
  }
}
