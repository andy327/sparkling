package com.sparkling.row.schema

/** A single named field within a [[RecordSchema]].
  *
  * @param name field name
  * @param valueType runtime type description for the field's values
  * @param nullable whether the field may be null / missing
  */
final case class SchemaField(name: String, valueType: ValueType, nullable: Boolean)
