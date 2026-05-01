package com.sparkling.row.types

/** A single named field within a [[RecordSchema]].
  *
  * @param name Field name
  * @param valueType Runtime type description for the field's values
  * @param nullable Whether the field may be null / missing
  */
final case class SchemaField(name: String, valueType: ValueType, nullable: Boolean)
