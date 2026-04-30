package com.sparkling.schema

import java.util.UUID

import scala.collection.immutable.ArraySeq

/** Represents a single validated field name.
  *
  * Invariants:
  *   - Name is exactly preserved (no trimming or renaming).
  *   - Name must be non-null, non-empty, and free of leading/trailing whitespace.
  */
final class Field private (val name: String) extends FieldsLike {

  private[this] val arr: Array[String] = Array(name)

  override def names: IndexedSeq[String] = ArraySeq.unsafeWrapArray(arr)

  override def fields: Fields = Fields.one(this)

  /** Convert this single name to a multi-name container. */
  def toFields: Fields = fields

  override def toString: String = s"[$name]"

  override def equals(other: Any): Boolean =
    other match {
      case f: Field => this.name == f.name
      case _        => false
    }

  override def hashCode(): Int = name.hashCode
}

object Field {

  /** Constructs a [[Field]] with strict validation. */
  def apply(name: String): Field = {
    require(name != null, "Field name must not be null")
    require(name.nonEmpty, "Field name must be non-empty")
    require(name == name.trim, s"Field name must not have leading/trailing whitespace: '$name'")
    new Field(name)
  }

  /** Creates a temporary [[Field]] with a short UUID suffix, e.g., `tmp-9f2c1a8b3d4e5f67`.
    *
    * Use for intermediate/throwaway columns that will be discarded later. Hyphens are removed and the suffix is
    * truncated to 16 hex chars for readability.
    */
  def tmp(prefix: String = "tmp-"): Field = {
    require(prefix != null && prefix.nonEmpty, "Prefix must be non-empty")
    require(prefix == prefix.trim, s"Prefix must be pre-trimmed: '$prefix'")
    val id = UUID.randomUUID().toString.replace("-", "").take(16)
    Field(prefix + id)
  }
}
