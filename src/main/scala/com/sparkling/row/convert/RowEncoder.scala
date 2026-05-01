package com.sparkling.row.convert

import com.sparkling.row.Row
import com.sparkling.row.codec.ValueCodec
import com.sparkling.row.types.{RecordSchema, ValueType}
import com.sparkling.schema.Fields

/** Typeclass for encoding a value of type `T` as a [[Row]].
  *
  * A `RowEncoder` defines how a typed Scala value should be represented as a [[Row]]. Encoders declare the schema
  * shape (field names and arity) of the rows they produce.
  *
  * When the desired output schema differs from the declared one, the encoder can be re-bound via [[alignTo]] to emit
  * rows with the requested field names while preserving the original value ordering.
  */
trait RowEncoder[T] extends Serializable { self =>

  /** Declared schema for the produced row. */
  def declared: Fields

  /** Runtime schema (names + value types + nullability) for the produced row. */
  def schema: RecordSchema

  /** Encodes to a row in declared order. */
  def to(t: T): Row

  /** Re-binds the output schema to `actual`.
    *
    * Supported alignment cases:
    *   1. same arity: rebind field names while preserving positional value order
    *   2. `actual.size == 1` and `declared.size > 1`: wrap the produced row as a nested object under the single
    *      requested output field
    */
  def alignTo(actual: Fields): RowEncoder[T] =
    if (actual.size == declared.size) {
      val renamedSchema = schema.rename(declared.names.zip(actual.names).toMap)
      val identityOrdinals = Array.tabulate(declared.size)(identity)

      new RowEncoder[T] {
        override val declared: Fields = actual
        override val schema: RecordSchema = renamedSchema

        override def to(t: T): Row = {
          val base = self.to(t)
          if (base.schema == actual) base
          else Row.view(base, actual, identityOrdinals)
        }

        override def alignTo(actual2: Fields): RowEncoder[T] =
          self.alignTo(actual2)
      }
    } else if (actual.size == 1 && declared.size > 1) {
      val outName = actual.names.head

      new RowEncoder[T] {
        override val declared: Fields = actual

        override val schema: RecordSchema =
          RecordSchema.single(
            name = outName,
            valueType = ValueType.ObjectType(self.schema),
            nullable = true
          )

        override def to(t: T): Row = {
          val inner = self.to(t)
          Row.fromArray(actual, Array[Any](inner))
        }

        override def alignTo(actual2: Fields): RowEncoder[T] =
          self.alignTo(actual2)
      }
    } else {
      throw new IllegalArgumentException(
        s"Cannot align output converter arity=${declared.size} to actual arity=${actual.size}"
      )
    }
}

object RowEncoder extends BoilerplateRowEncoderInstances {

  private[convert] def tupleFields(n: Int): Fields =
    Fields.fromArray(Array.tabulate(n)(i => s"_${i + 1}"))

  implicit val forUnit: RowEncoder[Unit] = new RowEncoder[Unit] {
    override val declared: Fields = Fields.empty
    override val schema: RecordSchema = RecordSchema.empty
    override def to(t: Unit): Row = Row.fromArray(Fields.empty, Array.empty[Any])
  }

  /** Derives a `RowEncoder[T]` from a `ValueCodec[T]`.
    *
    * For object-shaped codecs (where the encoded value is itself a [[Row]]), the encoder uses the nested schema
    * directly. For all other codecs, the encoder produces a single-column row using `ValueCodec.encode` and propagates
    * `ValueCodec.nullable` and `ValueCodec.valueType` into the runtime schema.
    */
  implicit def fromValueCodec[T](implicit vc: ValueCodec[T]): RowEncoder[T] =
    vc.valueType match {
      case ValueType.ObjectType(objSchema) =>
        val objFields = Fields.fromSeq(objSchema.columnNames)

        new RowEncoder[T] {
          override val declared: Fields = objFields
          override val schema: RecordSchema = objSchema

          override def to(t: T): Row =
            vc.encode(t) match {
              case null =>
                Row.fromArray(objFields, Array.fill[Any](objSchema.size)(null))
              case r: Row =>
                if (r.schema == objFields) r
                else r.alignTo(objFields, copy = false)
              case other =>
                throw new ClassCastException(
                  s"Expected base Row for ObjectType, got ${other.getClass.getName}"
                )
            }
        }

      case _ =>
        new RowEncoder[T] {
          override val declared: Fields = tupleFields(1)

          override val schema: RecordSchema =
            RecordSchema.single(
              name = declared.names.head,
              valueType = vc.valueType,
              nullable = vc.nullable
            )

          override def to(t: T): Row =
            Row.fromArray(declared, Array[Any](vc.encode(t)))
        }
    }
}
