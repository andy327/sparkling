package com.sparkling.row.types

/** Sealed ADT describing the runtime type of a value stored in a [[com.sparkling.row.Row]].
  *
  * Covers primitives (`StringType`, `IntType`, `LongType`, `DoubleType`, `BooleanType`) and recursive composite types
  * (`ArrayType`, `MapType`, `ObjectType`). Nullability is modeled separately at the field level via [[SchemaField]].
  */
sealed trait ValueType

object ValueType {

  // primitives
  case object StringType extends ValueType
  case object IntType extends ValueType
  case object LongType extends ValueType
  case object DoubleType extends ValueType
  case object BooleanType extends ValueType

  /** An ordered collection type (e.g. List/Vector/Seq). */
  final case class ArrayType(elementType: ValueType) extends ValueType

  /** A key/value map type. */
  final case class MapType(keyType: ValueType, valueType: ValueType) extends ValueType

  /** A nested object/record type with its own schema. */
  final case class ObjectType(schema: RecordSchema) extends ValueType
}
