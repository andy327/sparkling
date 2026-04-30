package com.sparkling.schema

/** Read-only abstraction over one or more validated field names.
  *
  * Implemented by [[Field]] (exactly one name) and [[Fields]] (zero or more). Provides a uniform view (`names`,
  * `arity`) and convenience transforms (`prefix`, `postfix`) that always yield a multi-field [[Fields]].
  */
trait FieldsLike extends Serializable {

  /** Underlying, ordered, validated names. */
  def names: IndexedSeq[String]

  /** Number of names represented. */
  final def arity: Int = names.size

  /** True if this represents exactly one name. */
  final def isSingle: Boolean = arity == 1

  /** True if the given name exists within this set. */
  def contains(name: String): Boolean = names.contains(name)

  /** Add a prefix to each name, returning a new multi-name container. */
  final def prefix(p: String): Fields = {
    val out = new Array[String](names.size)
    var i = 0
    while (i < names.size) {
      out(i) = p + names(i)
      i += 1
    }
    Fields.fromArray(out)
  }

  /** Add a postfix to each name, returning a new multi-name container. */
  final def postfix(p: String): Fields = {
    val out = new Array[String](names.size)
    var i = 0
    while (i < names.size) {
      out(i) = names(i) + p
      i += 1
    }
    Fields.fromArray(out)
  }

  /** Normalized multi-name representation. */
  def fields: Fields

  /** Compact, schema-like string form, e.g., `[a,b,c]`. */
  override def toString: String = s"[${names.mkString(",")}]"
}
