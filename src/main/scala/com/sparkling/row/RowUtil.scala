package com.sparkling.row

import com.sparkling.schema.Fields

/** Utilities for defensive checks and simple projections against [[Row]] and [[com.sparkling.schema.Fields]].
  *
  * By design, [[Row]] treats explicit `null` as missing for lookups (`get` returns `None`), so "present" here means
  * "present and non-null".
  */
object RowUtil {

  /** Returns true if and only if all columns in `fields` are present in `row` and non-null. */
  def allPresent(row: Row, fields: Fields): Boolean =
    // use schema index when possible (fast), otherwise fallback to name lookup.
    // if row.schema contains all fields, we can do ordinal checks.
    if (row.schema.containsAll(fields)) {
      val ords = fields.ordinalsIn(row.schema)
      allPresentAt(row, ords)
    } else {
      fields.names.forall(row.contains)
    }

  /** Fast-path: returns true if and only if all ordinals are non-null. */
  def allPresentAt(row: Row, ordinals: Array[Int]): Boolean = {
    var i = 0
    while (i < ordinals.length) {
      if (row.isNullAt(ordinals(i))) return false
      i += 1
    }
    true
  }

  /** Reads raw values (Some/None) for `fields` from `row`, preserving order (useful for debugging and assertions). */
  def raw(row: Row, fields: Fields): Vector[Option[Any]] =
    fields.names.map(row.get).toVector

  /** Compute ordinals of `target` fields in `source` schema, allowing missing fields.
    *
    * Returns an array of length target.size:
    * - if target(i) exists in source, ordinals(i) = index
    * - otherwise, ordinals(i) = -1
    */
  def ordinalsAllowMissing(target: Fields, source: Fields): Array[Int] = {
    val out = new Array[Int](target.size)
    var i = 0
    while (i < target.size) {
      val name = target.names(i)
      out(i) = source.index.getOrElse(name, -1)
      i += 1
    }
    out
  }
}
