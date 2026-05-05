package com.sparkling.frame

import org.apache.spark.sql.{Column, DataFrame}

import com.sparkling.schema.{Field, Fields}

/** A planned aggregation: an output column name paired with its Spark SQL aggregation expression. */
sealed trait PlannedAgg {
  def outName: String
  def expr: Column
}

object PlannedAgg {

  /** A single-output aggregation that writes one expression into one output column.
    *
    * @param outName name of the output column
    * @param expr Spark SQL aggregation expression
    */
  final case class Direct(outName: String, expr: Column) extends PlannedAgg

  /** A multi-output aggregation.
    *
    * The expression is computed into a temporary column `tmp` (typically a struct), then unpacked into the provided
    * output fields via `Frame.unpack`. The temporary column is dropped after unpacking.
    *
    * @param tmp temporary column used for the packed result
    * @param out output fields to unpack into
    * @param expr Spark aggregate expression producing a struct-like value
    */
  final case class Packed(tmp: Field, out: Fields, expr: Column) extends PlannedAgg {
    override def outName: String = tmp.name
    def unpack(df: DataFrame): DataFrame = Frame(df).unpack(tmp -> out).df
  }
}
