package com.sparkling.row.convert

import com.sparkling.row.Row
import com.sparkling.row.codec.ValueCodec
import com.sparkling.row.types.ValueType
import com.sparkling.schema.Fields

/** Typeclass for decoding a [[Row]] into a value of type `T`.
  *
  * A `RowDecoder` defines how to interpret the values of a [[Row]] as a typed Scala value. Decoders may optionally
  * declare the schema shape (field names and arity) they expect.
  *
  * When the runtime schema differs from the declared one, the decoder can be re-bound to the actual schema via
  * [[alignTo]]. This allows decoding to work even when field order or structure differs, including cases where the
  * declared fields are nested inside a single column.
  */
trait RowDecoder[T] extends Serializable { self =>

  /** Declared schema shape for this decoder.
    *
    * - None: accepts any Row (e.g. RowDecoder[Row], RowDecoder[Unit])
    * - Some(fields): declared arity for alignment/planning
    */
  def declared: Option[Fields]

  /** Decodes a value from a row already in the decoder's "actual" shape. */
  def from(row: Row): T

  /** Re-binds this decoder to the actual column layout seen at runtime.
    *
    * Two cases are supported:
    *   1. Same arity (`actual.size == declared.size`): the decoder is re-declared under the new field names while
    *      preserving positional decoding.
    *   2. Single-column nesting (`actual.size == 1`, `declared.size > 1`): the single actual column is treated as a
    *      nested [[Row]] (an object-typed cell), and decoding projects from it. Used when a multi-field value has been
    *      nested under one output column by [[com.sparkling.row.codec.RecordCodec.withFields]].
    *
    * Any other arity combination throws `IllegalArgumentException` at alignment time.
    */
  def alignTo(actual: Fields): RowDecoder[T] =
    declared match {
      case None =>
        this

      case Some(decl) if actual.size == decl.size =>
        new RowDecoder[T] {
          override val declared: Option[Fields] = Some(actual)
          override def from(row: Row): T = self.from(row)
          override def alignTo(actual2: Fields): RowDecoder[T] = self.alignTo(actual2)
        }

      case Some(decl) if actual.size == 1 && decl.size > 1 =>
        val nestedName = actual.names(0)
        new RowDecoder[T] {
          override val declared: Option[Fields] = Some(actual)

          override def from(row: Row): T = {
            val nested = row.getAt(0) match {
              case null   => throw new NoSuchElementException(s"Nested field '$nestedName' was null")
              case r: Row => r
              case other  =>
                throw new ClassCastException(
                  s"Expected nested Row in '$nestedName', got ${other.getClass.getName}"
                )
            }
            val projected = nested.alignTo(decl, copy = false)
            self.from(projected)
          }

          override def alignTo(actual2: Fields): RowDecoder[T] = self.alignTo(actual2)
        }

      case Some(decl) =>
        throw new IllegalArgumentException(
          s"Cannot align converter declared arity=${decl.size} to actual arity=${actual.size}. " +
            s"declared=${decl.names}, actual=${actual.names}"
        )
    }
}

object RowDecoder extends BoilerplateRowDecoderInstances {

  private[convert] def tupleFields(n: Int): Fields =
    Fields.fromArray(Array.tabulate(n)(i => s"_${i + 1}"))

  implicit val forUnit: RowDecoder[Unit] = new RowDecoder[Unit] {
    override val declared: Option[Fields] = None
    override def from(row: Row): Unit = ()
  }

  implicit val forRow: RowDecoder[Row] = new RowDecoder[Row] {
    override val declared: Option[Fields] = None
    override def from(row: Row): Row = row
  }

  /** Derives a `RowDecoder[T]` from a `ValueCodec[T]`.
    *
    * For object-shaped codecs (where the cell value is itself a [[Row]]), the decoder declares the nested schema and
    * reads by name. For all other codecs, the decoder declares a single positional field and delegates to
    * [[com.sparkling.row.codec.ValueCodec.decodeUnsafe]] — no `Either` allocation on the hot path.
    */
  implicit def fromValueCodec[T](implicit vc: ValueCodec[T]): RowDecoder[T] =
    vc.valueType match {
      case ValueType.ObjectType(schema) =>
        val itemFields = Fields.fromSeq(schema.columnNames)

        new RowDecoder[T] {
          override val declared: Option[Fields] = Some(itemFields)

          override def from(row: Row): T = {
            val projected =
              if (row.schema == itemFields) row
              else row.alignTo(itemFields, copy = false)
            vc.decodeUnsafe(projected)
          }

          override def alignTo(actual: Fields): RowDecoder[T] =
            declared match {
              case Some(decl) if actual.size == decl.size =>
                val identityOrdinals = Array.tabulate(decl.size)(identity)
                new RowDecoder[T] {
                  override val declared: Option[Fields] = Some(actual)
                  override def from(row: Row): T = {
                    val projected =
                      if (row.schema == decl) row
                      else Row.view(row, decl, identityOrdinals)
                    vc.decodeUnsafe(projected)
                  }
                  override def alignTo(actual2: Fields): RowDecoder[T] =
                    RowDecoder.fromValueCodec(vc).alignTo(actual2)
                }
              case _ =>
                super.alignTo(actual)
            }
        }

      case _ =>
        new RowDecoder[T] {
          override val declared: Option[Fields] = Some(tupleFields(1))
          override def from(row: Row): T = vc.decodeUnsafe(row.getAt(0))
        }
    }
}
