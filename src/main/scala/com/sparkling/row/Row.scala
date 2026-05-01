package com.sparkling.row

import com.sparkling.schema.{Field, Fields}

/** Minimal row abstraction for pure-Scala tests and planning.
  *
  * Null handling:
  *   - Implementations must treat explicit `null` values as missing: `get(...)` returns `None` when the value is null.
  */
trait Row {

  /** Schema for this row, in the same order as [[values]]. */
  def schema: Fields

  /** Underlying values in [[schema]] order.
    *
    * WARNING: for array-backed rows this is the live backing array (mutable). For view-backed rows (ProjectedRow)
    * this returns a freshly projected copy aligned to [[schema]]. Treat the result as read-only in all cases.
    */
  def values: Array[Any]

  /** Row width. */
  final def arity: Int = schema.size

  /** Safe lookup: None if missing or value is null. */
  def get(field: String): Option[Any]

  /** Fast ordinal access (may return null). */
  def getAt(i: Int): Any

  /** True if the value at ordinal i is null. */
  def isNullAt(i: Int): Boolean

  /** Type-aware helper. */
  final def getAs[A](field: String): Option[A] =
    get(field).map(_.asInstanceOf[A])

  /** Unsafe lookup: throws if missing or null. */
  final def applyUnsafe(field: String): Any =
    get(field).getOrElse(throw new NoSuchElementException(s"Missing or null field: $field"))

  /** Existence check (present & non-null). */
  final def contains(field: String): Boolean = get(field).isDefined

  /** Copy-based ordinal projection.
    *
    * @param outSchema Schema for the output row (must match ordinals length).
    * @param ordinals Ordinals into *this* row's schema; may include -1 to mean "missing => null".
    */
  final def selectAt(outSchema: Fields, ordinals: Array[Int]): Row = {
    require(
      outSchema.size == ordinals.length,
      s"outSchema.size ${outSchema.size} != ordinals.length ${ordinals.length}"
    )

    val out = new Array[Any](ordinals.length)
    copyProjectedInto(ordinals, out)
    Row.fromArray(outSchema, out)
  }

  /** Zero-copy projection view.
    *
    * This avoids allocating a new values array; reads are forwarded into this row by ordinal. Use this in hot paths
    * (e.g., schema alignment inside streaming operations).
    *
    * IMPORTANT: Because this is a view, if the underlying row's values are mutated, this view reflects those changes.
    * Calling [[values]] on the resulting row returns a freshly projected copy (not the base array), so [[values]] is
    * always safe to use but is not zero-copy for view rows.
    */
  final def viewAt(outSchema: Fields, ordinals: Array[Int]): Row = {
    require(
      outSchema.size == ordinals.length,
      s"outSchema.size ${outSchema.size} != ordinals.length ${ordinals.length}"
    )
    Row.view(base = this, outSchema = outSchema, ordinals = ordinals)
  }

  /** Aligns this row to a target schema (reorder + allow missing).
    *
    * Missing columns become nulls.
    *
    * @param target the desired schema order.
    * @param copy if true, returns a copy-based Row; if false, returns a view-based Row.
    */
  final def alignTo(target: Fields, copy: Boolean = false): Row = {
    val ords = RowUtil.ordinalsAllowMissing(target, this.schema)
    if (copy) selectAt(target, ords) else viewAt(target, ords)
  }

  /** Fill `out` with values projected by `ordinals`.
    *
    * - ordinals(i) >= 0 => out(i) = this.getAt(ordinals(i))
    * - ordinals(i) == -1 => out(i) = null
    *
    * This enables array reuse in tight loops.
    */
  final def copyProjectedInto(ordinals: Array[Int], out: Array[Any]): Unit = {
    require(out.length == ordinals.length, s"out.length ${out.length} != ordinals.length ${ordinals.length}")
    var i = 0
    while (i < ordinals.length) {
      val j = ordinals(i)
      out(i) = if (j >= 0) getAt(j) else null
      i += 1
    }
  }

  /** Returns a new [[Row]] with the given extra fields appended.
    *
    * The new row's schema is the merge of the current [[schema]] and `extraFields`, and the values are the
    * concatenation of the current [[values]] and `extraValues`.
    *
    * @param extraFields Fields to append.
    * @param extraValues values for those fields, in the same order.
    */
  final def withValues(extraFields: Fields, extraValues: Array[Any]): Row = {
    require(
      extraFields.size == extraValues.length,
      s"extra fields size ${extraFields.size} != extra values size ${extraValues.length}"
    )

    // For derivatives and intermediate columns we expect names not to overlap; this is a defensive check.
    val merged = Fields.merge(schema, extraFields)
    require(
      merged.size == schema.size + extraFields.size,
      s"withValues would introduce duplicate field names: base=${schema.names}, extra=${extraFields.names}"
    )

    val out = new Array[Any](schema.size + extraValues.length)
    var i = 0
    while (i < schema.size) {
      out(i) = getAt(i)
      i += 1
    }
    System.arraycopy(extraValues, 0, out, schema.size, extraValues.length)

    Row.fromArray(merged, out)
  }

  /** Convenience overload of [[withValues]] for adding a single field/value pair.
    *
    * @param extraField Field to append (must not already exist in the row).
    * @param extraValue value for that field.
    */
  final def withValue(extraField: Field, extraValue: Any): Row =
    withValues(Fields.one(extraField), Array[Any](extraValue))
}

object Row {

  /** Concrete, schema-backed array row implementation. */
  final private class ArrayRow(val schema: Fields, val values: Array[Any]) extends Row {

    override def get(field: String): Option[Any] =
      schema.index.get(field) match {
        case None    => None
        case Some(i) =>
          val v = values(i)
          if (v == null) None else Some(v)
      }

    override def getAt(i: Int): Any = values(i)

    override def isNullAt(i: Int): Boolean = values(i) == null

    override def toString: String = s"Row(schema=${schema.names.mkString(",")})"
  }

  /** A projection view over another Row.
    *
    * Reads are forwarded into the base row by ordinal. Calling [[values]] materializes a correctly-projected copy
    * aligned to [[schema]] - it does not return the base array.
    */
  final private class ProjectedRow(
      val schema: Fields,
      base: Row,
      ordinals: Array[Int]
  ) extends Row {

    /** Returns a projected copy of values aligned to [[schema]].
      *
      * Unlike ArrayRow, this allocates on each call. Prefer [[getAt]] or [[get]] in hot paths.
      */
    override def values: Array[Any] = {
      val out = new Array[Any](ordinals.length)
      var i = 0
      while (i < ordinals.length) {
        val j = ordinals(i)
        out(i) = if (j >= 0) base.getAt(j) else null
        i += 1
      }
      out
    }

    override def get(field: String): Option[Any] =
      schema.index.get(field) match {
        case None    => None
        case Some(i) =>
          val j = ordinals(i)
          if (j < 0) None
          else {
            val v = base.getAt(j)
            if (v == null) None else Some(v)
          }
      }

    override def getAt(i: Int): Any = {
      val j = ordinals(i)
      if (j < 0) null else base.getAt(j)
    }

    override def isNullAt(i: Int): Boolean = {
      val j = ordinals(i)
      j < 0 || base.isNullAt(j)
    }

    override def toString: String = s"Row(view schema=${schema.names.mkString(",")})"
  }

  /** Build a projection view (no copying). */
  def view(base: Row, outSchema: Fields, ordinals: Array[Int]): Row =
    new ProjectedRow(outSchema, base, ordinals)

  /** Map-backed row that treats nulls as None.
    *
    * Keys are converted to a deterministic schema (sorted by field name) and values are aligned with that schema.
    */
  def fromMap(m: Map[String, Any]): Row = {
    val names = m.keys.toArray.sorted
    val schema = Fields.fromArray(names)

    val arr = new Array[Any](names.length)
    var i = 0
    while (i < names.length) {
      arr(i) = m(names(i))
      i += 1
    }

    fromArray(schema, arr)
  }

  /** Array-backed row with schema that treats nulls as None.
    *
    * IMPORTANT: the array is used as-is (no defensive copy). Treat it as immutable after passing it here.
    */
  def fromArray(schema: Fields, values: Array[Any]): Row = {
    require(schema.size == values.length, s"Schema size ${schema.size} != values size ${values.length}")
    new ArrayRow(schema, values)
  }

  /** Convenience constructor from any Seq (copies into an array). */
  def fromSeq(schema: Fields, values: Seq[Any]): Row =
    fromArray(schema, values.toArray)
}
