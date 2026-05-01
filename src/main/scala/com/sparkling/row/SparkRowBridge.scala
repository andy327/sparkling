package com.sparkling.row

import scala.collection.immutable.ArraySeq

import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row => SparkRow}

import com.sparkling.row.types.{RecordSchema, ValueType}
import com.sparkling.schema.Fields

/** Internal bridge between Spark SQL struct rows and sparkling [[Row]]s.
  *
  * This bridge is intentionally narrow: it converts whole struct rows in schema order. Higher-level code is
  * responsible for any field selection/reordering before invoking it.
  */
object SparkRowBridge {

  /** Converts a full Spark struct row into a sparkling [[Row]] with matching field names and order.
    *
    * Nested structs are converted recursively. Other values are carried through as-is.
    */
  def fromSparkRow(row: SparkRow, schema: StructType): Row =
    if (row == null) null
    else {
      val n = schema.fields.length
      val arr = new Array[Any](n)

      var i = 0
      while (i < n) {
        val dt = schema.fields(i).dataType
        val raw = if (row.isNullAt(i)) null else row.get(i)

        arr(i) =
          if (raw == null) null
          else
            dt match {
              case st: StructType => fromSparkRow(raw.asInstanceOf[SparkRow], st)
              case _              => raw
            }

        i += 1
      }

      Row.fromArray(Fields.fromSeq(schema.fieldNames.toSeq), arr)
    }

  /** Converts a sparkling [[Row]] into a Spark SQL row using the provided runtime schema.
    *
    * Nested object-typed fields are converted recursively. Other values are carried through as-is.
    */
  def toSparkRow(row: Row, schema: RecordSchema): SparkRow = {
    val n = schema.size
    val arr = new Array[Any](n)

    var i = 0
    while (i < n) {
      val sf = schema.columns(i)
      val raw =
        if (row == null || row.isNullAt(i)) null
        else row.getAt(i)

      arr(i) =
        if (raw == null) null
        else
          sf.valueType match {
            case ValueType.ObjectType(nestedSchema) =>
              toSparkRow(raw.asInstanceOf[Row], nestedSchema)
            case _ =>
              raw
          }

      i += 1
    }

    SparkRow.fromSeq(ArraySeq.unsafeWrapArray(arr))
  }
}
