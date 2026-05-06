package com.sparkling.frame

import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.types._
import org.apache.spark.sql.{functions => sqlf, Column, DataFrame, Row => SparkRow}

import com.sparkling.row.SparkRowBridge
import com.sparkling.schema.{Fields, SparkSchema}

/** Executes reduce-side streaming group operations.
  *
  * Conceptually, this performs a streaming group-by using Spark's grouped Dataset API:
  *   - The input DataFrame is projected into two nested structs:
  *       * a key struct containing the grouping columns (or a dummy constant key for `streamAll`)
  *       * a value struct containing any sort fields followed by the stream operation's input fields
  *   - Spark groups rows by the key struct and presents each group as a single iterator of value rows.
  *   - This executor walks each group iterator once, invoking user-defined stream logic as rows are encountered.
  *
  * Stream semantics:
  *   - Rows within each group are always presented in a stable order:
  *       * If `maybeSort` is defined, rows are ordered by the specified sort fields (with `reverse` applying only to
  *         those fields).
  *       * Otherwise, a stable fallback ordering over the value fields is used.
  *   - Groups are processed incrementally; no group is fully materialized unless user code explicitly does so.
  *
  * Output shape:
  *   - For keyed group operations, each emitted row is prefixed with the grouping key columns, yielding rows of shape
  *     `keys ++ out`.
  *   - For `streamAll`, no key columns are emitted; the output consists solely of the stream operation's results.
  */
private[frame] object StreamExecutor {

  type GroupProcessor = (SparkRow, Iterator[SparkRow]) => Iterator[SparkRow]

  /** A non-empty key schema used to support "stream all rows" plans.
    *
    * Spark cannot construct an `ExpressionEncoder` for an empty `StructType`, so `streamAll` is implemented by
    * grouping on a single constant dummy field.
    */
  private val dummyKeySchema: StructType =
    StructType(Seq(StructField("__dummy", IntegerType, nullable = false)))

  /** Executes a stream plan and returns the resulting `DataFrame`.
    *
    * @param df input DataFrame
    * @param keys grouping key fields (empty implies all rows are grouped together)
    * @param op stream operation (typed iterator transform)
    * @param maybeSort optional sort fields + reverse flag; reverse applies only to those sort fields
    */
  def run(
      df: DataFrame,
      keys: Fields,
      op: StreamOperation,
      maybeSort: Option[(Fields, Boolean)]
  ): DataFrame = {
    val inputSchema = df.schema
    val isGroupAll = keys.isEmpty

    // Spark cannot group by an empty struct, so for streamAll we synthesize a dummy key column with a constant value.
    val (keyFields, keySchema): (Fields, StructType) =
      if (isGroupAll) (Fields("__dummy"), dummyKeySchema)
      else (keys, selectSchema(inputSchema, keys))

    val (sortFields, reverse): (Fields, Boolean) = maybeSort match {
      case Some((fs, rev)) => (fs, rev)
      case None            => (Fields.empty, false)
    }

    // carry sort fields through the grouped value struct so Spark can sort before iteration
    val valueFields: Fields = Fields.merge(sortFields, op.in)
    val valueSchema: StructType = selectSchema(inputSchema, valueFields)

    // bind converters once per plan
    val alignedDec = op.dec.alignTo(op.in)
    val alignedEnc = op.enc.alignTo(op.out)

    val outputSchema: StructType = SparkSchema.toStructType(alignedEnc.schema)

    // If no explicit sort fields are provided, fall back to sorting by the full grouped value struct.
    val sortExpr: Column = (sortFields, reverse) match {
      case (fs, _) if fs.isEmpty => sqlf.struct(valueFields.names.map(sqlf.col): _*)
      case (fs, false)           => sqlf.struct(fs.names.map(sqlf.col): _*).asc
      case (fs, true)            => sqlf.struct(fs.names.map(sqlf.col): _*).desc
    }

    val keyCol: Column =
      if (isGroupAll) sqlf.struct(sqlf.lit(0).as("__dummy")).as("__k")
      else sqlf.struct(keyFields.names.map(sqlf.col): _*).as("__k")

    val valueCol: Column = sqlf.struct(valueFields.names.map(sqlf.col): _*).as("__v")

    val keyValueDf = df.select(keyCol, valueCol)

    val finalSchema: StructType =
      if (isGroupAll) outputSchema
      else StructType(keySchema.fields ++ outputSchema.fields)

    val processGroup: GroupProcessor =
      (keyRow: SparkRow, valueIter: Iterator[SparkRow]) => {
        val ctx = op.init()

        val inIter: Iterator[op.In] =
          valueIter.map { valueRow =>
            val baseValue = SparkRowBridge.fromSparkRow(valueRow, valueSchema)
            val baseIn =
              if (sortFields.isEmpty) baseValue
              else baseValue.alignTo(op.in, copy = false)
            alignedDec.from(baseIn)
          }

        op.step(ctx, inIter).map { outVal =>
          val outBase = alignedEnc.to(outVal)
          val outSpark = SparkRowBridge.toSparkRow(outBase, alignedEnc.schema)
          if (isGroupAll) outSpark else concatRows(keyRow, outSpark)
        }
      }

    val keyEnc = ExpressionEncoder(keySchema)
    val valueEnc = ExpressionEncoder(valueSchema)
    val finalEnc = ExpressionEncoder(finalSchema)

    keyValueDf
      .groupByKey((r: SparkRow) => r.getStruct(0))(keyEnc)
      .mapValues((r: SparkRow) => r.getStruct(1))(valueEnc)
      .flatMapSortedGroups(sortExpr)(processGroup)(finalEnc)
      .toDF()
  }

  private def selectSchema(schema: StructType, fields: Fields): StructType =
    StructType(fields.names.iterator.map(n => schema(schema.fieldIndex(n))).toArray)

  private def concatRows(r1: SparkRow, r2: SparkRow): SparkRow = {
    val k = r1.size
    val o = r2.size
    val arr = new Array[Any](k + o)

    var i = 0
    while (i < k) { arr(i) = r1.get(i); i += 1 }

    var j = 0
    while (j < o) { arr(k + j) = r2.get(j); j += 1 }

    SparkRow.fromSeq(arr.toSeq)
  }
}
