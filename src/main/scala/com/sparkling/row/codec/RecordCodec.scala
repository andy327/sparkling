package com.sparkling.row.codec

import shapeless._
import shapeless.labelled.{field, FieldType}

import com.sparkling.row.Row
import com.sparkling.row.types.ValueType.ObjectType
import com.sparkling.row.types.{RecordSchema, SchemaField}
import com.sparkling.schema.Fields

/** Typeclass describing how to encode/decode a Scala value `A` as a [[Row]] plus a runtime
  * [[com.sparkling.row.types.RecordSchema]].
  *
  * `ValueCodec` handles cell-level typing, coercion, and encode/decode. `RecordCodec` adds structured, schema-aware
  * record encoding on top.
  *
  * `withFields` supports rebinding the codec's schema to declared output [[com.sparkling.schema.Fields]]:
  *   - if outputs.size == schema.size: rename columns one-to-one (preserves order)
  *   - if outputs.size == 1 && schema.size > 1: nest the object under that single output name
  *   - otherwise: throw
  */
trait RecordCodec[A] extends Serializable {

  /** Runtime schema produced by this codec.
    *
    * Column names, types, and nullability match the fields written by [[encodeRecord]].
    */
  def schema: RecordSchema

  /** Encodes `a` as a [[Row]] whose schema matches [[schema]].
    *
    * The returned row is owned by the caller and may be read freely. Implementations are not required to produce a
    * defensive copy of any internal state.
    */
  def encodeRecord(a: A): Row

  /** Encodes `a` directly to a values array without wrapping in a [[Row]].
    *
    * The default implementation delegates to [[encodeRecord]] and extracts the backing array. Implementations that
    * build the array directly (e.g., `RecordCodec.renamed`) override this to skip the intermediate [[Row]] allocation
    * entirely.
    *
    * Callers must treat the returned array as immutable.
    */
  def encodeValues(a: A): Array[Any] = encodeRecord(a).values

  /** Decodes a value of type `A` from `row`, reading fields by name according to [[schema]].
    *
    * Returns `Right(a)` on success, or `Left(throwable)` if a required field is missing, null, or cannot be coerced to
    * the expected type.
    */
  def decodeRecord(row: Row): Either[Throwable, A]

  /** Rebinds this codec to the declared output fields. */
  def withFields(outputs: Fields): RecordCodec[A]
}

object RecordCodec {

  /** Convenience: build a [[Fields]] instance from schema column names. */
  private def fieldsFor(schema: RecordSchema): Fields =
    Fields.fromSeq(schema.columnNames)

  /** Rebind helper: mapping oldName -> newName preserving order. */
  private def renameMapping(oldNames: Seq[String], newNames: Seq[String]): Map[String, String] =
    oldNames.zip(newNames).toMap

  /** Wraps a codec by renaming its schema and rewriting the encoded row with the new field names. */
  private def renamed[A](base: RecordCodec[A], newSchema: RecordSchema): RecordCodec[A] =
    new RecordCodec[A] {
      val schema: RecordSchema = newSchema

      private val oldFields: Fields = fieldsFor(base.schema)
      private val newFields: Fields = fieldsFor(newSchema)

      def encodeRecord(a: A): Row = {
        val values = encodeValues(a)
        Row.fromArray(newFields, values)
      }

      override def encodeValues(a: A): Array[Any] = {
        val oldRow = base.encodeRecord(a)
        oldFields.names.iterator.map(n => oldRow.get(n).orNull).toArray
      }

      def decodeRecord(row: Row): Either[Throwable, A] = {
        // read values in new schema order and rebuild a row keyed by old schema names,
        // then delegate to base decoder.
        val values: Array[Any] = newFields.names.iterator.map(n => row.get(n).orNull).toArray
        val oldRow: Row = Row.fromArray(oldFields, values)
        base.decodeRecord(oldRow)
      }

      def withFields(outputs: Fields): RecordCodec[A] =
        RecordCodec.rebind(this, outputs)
    }

  /** Wrap a codec by nesting it under a single output name as an object. */
  private def nested[A](base: RecordCodec[A], outName: String): RecordCodec[A] =
    new RecordCodec[A] {
      val schema: RecordSchema =
        RecordSchema.single(outName, ObjectType(base.schema), nullable = true)

      def encodeRecord(a: A): Row = {
        val inner = base.encodeRecord(a)
        Row.fromArray(Fields(outName), Array(inner))
      }

      def decodeRecord(row: Row): Either[Throwable, A] =
        row.get(outName) match {
          case None         => Left(new NoSuchElementException(s"Missing or null field: $outName"))
          case Some(r: Row) =>
            base.decodeRecord(r)
          case Some(x) =>
            Left(new ClassCastException(s"Expected Row for nested object field '$outName', got ${x.getClass.getName}"))
        }

      def withFields(outputs: Fields): RecordCodec[A] =
        RecordCodec.rebind(this, outputs)
    }

  /** Rebinds a codec's column names to the declared output [[Fields]].
    *
    * Two cases:
    *   - Same arity: rename columns one-to-one in declaration order.
    *   - Outputs.size == 1, schema.size > 1: nest the multi-column codec under a single named column as an
    *     object-typed cell.
    */
  private def rebind[A](base: RecordCodec[A], outputs: Fields): RecordCodec[A] = {
    val outNames = outputs.names
    val inNames = base.schema.columnNames

    if (outNames.size == inNames.size) {
      val mapping = renameMapping(inNames, outNames)
      val newSchema = base.schema.rename(mapping)
      renamed(base, newSchema)
    } else if (outNames.size == 1 && inNames.size > 1) {
      nested(base, outNames.head)
    } else {
      throw new IllegalArgumentException(
        s"Cannot rebind RecordCodec: codec has ${inNames.size} columns (${inNames.mkString(",")}), " +
          s"but outputs has ${outNames.size} (${outNames.mkString(",")})"
      )
    }
  }

  /** Scalar codec: wraps a single value `A` as a one-column record named `_1`.
    *
    * The name `_1` is a placeholder; the column is renamed to its declared output field name when `withFields` is
    * called.
    */
  implicit def scalar[A](implicit vc: ValueCodec[A]): RecordCodec[A] =
    new RecordCodec[A] {
      val schema: RecordSchema =
        RecordSchema.single("_1", vc.valueType, nullable = true)

      def encodeRecord(a: A): Row =
        Row.fromArray(Fields("_1"), Array(vc.encode(a)))

      def decodeRecord(row: Row): Either[Throwable, A] =
        vc.decode(row.get("_1").orNull)

      def withFields(outputs: Fields): RecordCodec[A] =
        rebind(this, outputs)
    }

  // Shapeless derivation for case classes / products (field names preserved)

  /** RecordCodec for labelled HNil (empty product). */
  implicit val hnil: RecordCodec[HNil] =
    new RecordCodec[HNil] {
      val schema: RecordSchema = RecordSchema.empty
      def encodeRecord(a: HNil): Row =
        Row.fromArray(Fields.empty, Array.empty)
      def decodeRecord(row: Row): Either[Throwable, HNil] = Right(HNil)
      def withFields(outputs: Fields): RecordCodec[HNil] = rebind(this, outputs)
    }

  /** RecordCodec for labelled field head + tail. */
  implicit def hcons[K <: Symbol, V, T <: HList](implicit
      wk: Witness.Aux[K],
      vc: ValueCodec[V],
      tc: RecordCodec[T]
  ): RecordCodec[FieldType[K, V] :: T] =
    new RecordCodec[FieldType[K, V] :: T] {

      private val headName: String = wk.value.name

      val schema: RecordSchema = {
        val head = SchemaField(headName, vc.valueType, nullable = true)
        RecordSchema(head +: tc.schema.columns)
      }

      def encodeRecord(a: FieldType[K, V] :: T): Row = {
        val headValue: Any = vc.encode(a.head)
        val tailRow: Row = tc.encodeRecord(a.tail)

        val names = schema.columnNames

        val tailNames = tc.schema.columnNames
        val out = new Array[Any](1 + tailNames.size)
        out(0) = headValue
        var i = 0
        while (i < tailNames.size) {
          out(i + 1) = tailRow.get(tailNames(i)).orNull
          i += 1
        }

        Row.fromArray(Fields.fromSeq(names), out)
      }

      def decodeRecord(row: Row): Either[Throwable, FieldType[K, V] :: T] = {
        val headRaw: Any = row.get(headName).orNull
        for {
          hv <- vc.decode(headRaw)
          tv <- tc.decodeRecord(row)
        } yield field[K](hv) :: tv
      }

      def withFields(outputs: Fields): RecordCodec[FieldType[K, V] :: T] =
        rebind(this, outputs)
    }

  /** Derive RecordCodec for case classes / products via LabelledGeneric. */
  implicit def product[A, R <: HList](implicit
      gen: LabelledGeneric.Aux[A, R],
      rc: RecordCodec[R]
  ): RecordCodec[A] =
    new RecordCodec[A] {
      val schema: RecordSchema = rc.schema
      def encodeRecord(a: A): Row = rc.encodeRecord(gen.to(a))
      def decodeRecord(row: Row): Either[Throwable, A] = rc.decodeRecord(row).map(gen.from)
      def withFields(outputs: Fields): RecordCodec[A] = rebind(this, outputs)
    }
}
