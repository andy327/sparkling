package com.sparkling.schema

import scala.collection.immutable.ArraySeq

/** Represents an ordered, distinct, and validated set of field names.
  *
  * General-purpose schema fragment used to group input/output columns consistently.
  *
  * Invariants:
  *   - Names are exactly preserved (no trimming or renaming).
  *   - Names must be non-null, non-empty, and free of leading/trailing whitespace.
  *   - Names must be distinct within a single [[Fields]] instance.
  *
  * Performance:
  *   - Backed by an Array[String].
  *   - Provides a cached name -> ordinal index map.
  */
final class Fields private (private val arr: Array[String]) extends FieldsLike {

  override def fields: Fields = this

  /** Names view (does not copy). */
  override def names: IndexedSeq[String] =
    ArraySeq.unsafeWrapArray(arr)

  def size: Int = arr.length

  def isEmpty: Boolean = arr.isEmpty

  def nonEmpty: Boolean = !isEmpty

  /** Exposes the internal array for hot paths within core code.
    *
    * IMPORTANT: This is NOT a defensive copy. Treat as read-only.
    */
  private[schema] def toArrayUnsafe: Array[String] = arr

  /** Cached name -> ordinal index. */
  lazy val index: Map[String, Int] = {
    // pre-size builder a bit to reduce resizing
    val b = Map.newBuilder[String, Int]
    var i = 0
    while (i < arr.length) {
      b += (arr(i) -> i)
      i += 1
    }
    b.result()
  }

  /** Returns the ordinals of this Fields' names within `schema`.
    *
    * This is a performance helper: you can precompute ordinals once and then do fast ordinal-based reads/null checks.
    *
    * @throws java.util.NoSuchElementException if any name in `this` is not present in `schema`.
    */
  def ordinalsIn(schema: Fields): Array[Int] = {
    val out = new Array[Int](this.size)
    val schemaIdx = schema.index
    var i = 0
    while (i < out.length) {
      val name = arr(i)
      out(i) = schemaIdx.getOrElse(name, throw new NoSuchElementException(s"Field not found: $name"))
      i += 1
    }
    out
  }

  /** Returns the 0-based index of a field name, or `-1` if not present. */
  def indexOf(name: String): Int =
    index.getOrElse(name, -1)

  /** True if this schema contains the given field name. */
  def containsName(name: String): Boolean =
    index.contains(name)

  /** Returns the ordinal for name, or throws if not present. */
  def ordinalOf(name: String): Int =
    index.getOrElse(name, throw new NoSuchElementException(s"Field not found: $name"))

  /** Concatenates two validated collections (re-validates; duplicates will throw). */
  def ++(other: Fields): Fields = {
    val out = new Array[String](this.size + other.size)
    System.arraycopy(this.arr, 0, out, 0, this.size)
    System.arraycopy(other.arr, 0, out, this.size, other.size)
    Fields.fromArray(out)
  }

  /** Appends a single validated raw name (re-validates; duplicates will throw). */
  def :+(name: String): Fields = {
    val out = new Array[String](this.size + 1)
    System.arraycopy(this.arr, 0, out, 0, this.size)
    out(this.size) = name
    Fields.fromArray(out)
  }

  /** Appends a single validated [[Field]] (re-validates; duplicates will throw). */
  def :+(field: Field): Fields =
    this :+ field.name

  /** Returns a new [[Fields]] with the given fields removed, preserving order.
    *
    * Fields in `other` that are not present in this set are silently ignored.
    */
  def discard(other: Fields): Fields = {
    val drop = other.names.toSet
    Fields.fromSeq(names.filter(!drop(_)))
  }

  /** Returns a new [[Fields]] containing only the fields in `other`, preserving order from this set.
    *
    * Fields in `other` that are not present in this set are silently ignored.
    */
  def keep(other: Fields): Fields = {
    val retain = other.names.toSet
    Fields.fromSeq(names.filter(retain))
  }

  /** Returns the names as a Vector (allocates; prefer [[names]] when possible). */
  def toVector: Vector[String] =
    names.toVector

  /** Returns true if all field names in `other` are present in this set. */
  def containsAll(other: Fields): Boolean = {
    val idx = this.index
    val o = other.arr
    var i = 0
    while (i < o.length) {
      if (!idx.contains(o(i))) return false
      i += 1
    }
    true
  }

  override def equals(other: Any): Boolean =
    other match {
      case f: Fields =>
        val a1 = this.arr
        val a2 = f.arr
        if (a1.length != a2.length) false
        else {
          var i = 0
          while (i < a1.length) {
            if (a1(i) != a2(i)) return false
            i += 1
          }
          true
        }
      case _ => false
    }

  override def hashCode(): Int = {
    var h = 1
    var i = 0
    while (i < arr.length) {
      h = 31 * h + arr(i).hashCode
      i += 1
    }
    h
  }

  override def toString: String = s"[${arr.mkString(",")}]"
}

object Fields {

  /** Constructs a [[Fields]] instance with strict validation. */
  def apply(names: String*): Fields =
    fromArray(names.toArray)

  /** Construct from a sequence of field names (copies). */
  def fromSeq(names: Seq[String]): Fields =
    fromArray(names.toArray)

  /** Constructs from an array of field names.
    *
    * IMPORTANT: the array is used as-is (no defensive copy). Treat it as immutable afterwards.
    */
  def fromArray(names: Array[String]): Fields =
    fromArrayValidated(names)

  /** Empty field set (no names). */
  val empty: Fields = new Fields(Array.empty[String])

  /** Convenience: creates a single-field collection. */
  def one(f: Field): Fields =
    fromArray(Array(f.name))

  /** Creates multiple temporary fields with a short UUID suffix. */
  def tmp(n: Int = 1, prefix: String = "tmp-"): Fields = {
    require(n >= 1, s"n must be >= 1 (got $n)")
    val out = new Array[String](n)
    var i = 0
    while (i < n) {
      out(i) = Field.tmp(prefix).name
      i += 1
    }
    fromArray(out)
  }

  /** Merges multiple [[Fields]] into a single set, preserving first occurrence and order.
    *
    * Example:
    * {{{
    * val merged = Fields.merge(
    *   Fields("a", "b"),
    *   Fields("b", "c"),
    *   Fields("a", "d")
    * )
    * // merged.names == ArraySeq("a","b","c","d")
    * }}}
    */
  def merge(all: Fields*): Fields = {
    val seen = scala.collection.mutable.HashSet.empty[String]

    // upper bound allocation: sum of lengths
    var cap = 0
    var ai = 0
    while (ai < all.length) {
      cap += all(ai).size
      ai += 1
    }

    val out = new Array[String](cap)
    var outN = 0

    ai = 0
    while (ai < all.length) {
      val a = all(ai).arr
      var i = 0
      while (i < a.length) {
        val n = a(i)
        if (seen.add(n)) {
          out(outN) = n
          outN += 1
        }
        i += 1
      }
      ai += 1
    }

    if (outN == 0) empty
    else if (outN == out.length) fromArray(out)
    else fromArray(java.util.Arrays.copyOf(out, outN))
  }

  /** Computes the intersection of multiple [[Fields]] sets, preserving order from the first set. */
  def intersect(all: Fields*): Fields =
    if (all.isEmpty) empty
    else {
      // build common set from all tails
      val common = scala.collection.mutable.HashSet.empty[String]
      val headArr = all.head.arr

      // seed with head
      var i = 0
      while (i < headArr.length) {
        common += headArr(i)
        i += 1
      }

      // intersect with each other fields
      var ai = 1
      while (ai < all.length) {
        val cur = all(ai).arr
        val curSet = scala.collection.mutable.HashSet.empty[String]
        var j = 0
        while (j < cur.length) {
          curSet += cur(j)
          j += 1
        }
        common.filterInPlace(curSet.contains)
        ai += 1
      }

      // preserve head order
      val out = new Array[String](headArr.length)
      var outN = 0
      i = 0
      while (i < headArr.length) {
        val n = headArr(i)
        if (common.contains(n)) {
          out(outN) = n
          outN += 1
        }
        i += 1
      }

      if (outN == 0) empty
      else if (outN == out.length) fromArray(out)
      else fromArray(java.util.Arrays.copyOf(out, outN))
    }

  /** Internal constructor used by public builders (strict validation). */
  private def fromArrayValidated(names: Array[String]): Fields = {
    require(!names.contains(null), "Field names must not be null")

    var i = 0
    while (i < names.length) {
      val n = names(i)
      require(n.nonEmpty, "Field names must be non-empty")
      require(n == n.trim, s"Field name must not have leading/trailing whitespace: '$n'")
      i += 1
    }

    // duplicate check
    val seen = scala.collection.mutable.HashSet.empty[String]
    i = 0
    while (i < names.length) {
      val n = names(i)
      if (!seen.add(n))
        throw new IllegalArgumentException(s"Duplicate field names detected: ${names.mkString(",")}")
      i += 1
    }

    new Fields(names)
  }
}
