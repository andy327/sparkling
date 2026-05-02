package com.sparkling.schema

import org.apache.spark.sql.{types => SparkType}

import com.sparkling.row.types.{RecordSchema, SchemaField, ValueType}

/** Converts sparkling schema types ([[com.sparkling.row.types.ValueType ValueType]],
  * [[com.sparkling.row.types.RecordSchema RecordSchema]]) to their Spark SQL equivalents (`DataType`, `StructType`).
  *
  * This object is responsible only for schema translation. Row-level conversion between Spark rows and sparkling rows
  * is handled separately by [[com.sparkling.row.SparkRowBridge]].
  */
object SparkSchema {

  /** Convert a [[com.sparkling.row.types.ValueType ValueType]] into a Spark SQL `DataType`. */
  def toSparkType(vt: ValueType): SparkType.DataType =
    vt match {
      case ValueType.StringType         => SparkType.StringType
      case ValueType.IntType            => SparkType.IntegerType
      case ValueType.LongType           => SparkType.LongType
      case ValueType.DoubleType         => SparkType.DoubleType
      case ValueType.BooleanType        => SparkType.BooleanType
      case ValueType.ArrayType(elem)    => SparkType.ArrayType(toSparkType(elem), containsNull = true)
      case ValueType.MapType(k, v)      => SparkType.MapType(toSparkType(k), toSparkType(v), valueContainsNull = true)
      case ValueType.ObjectType(schema) => toStructType(schema)
    }

  /** Convert a [[com.sparkling.row.types.RecordSchema RecordSchema]] into a Spark `StructType`. */
  def toStructType(schema: RecordSchema): SparkType.StructType =
    SparkType.StructType(schema.columns.map(toStructField))

  /** Convert a [[com.sparkling.row.types.SchemaField SchemaField]] into a Spark `StructField`. */
  def toStructField(f: SchemaField): SparkType.StructField =
    SparkType.StructField(
      name = f.name,
      dataType = toSparkType(f.valueType),
      nullable = f.nullable
    )
}
