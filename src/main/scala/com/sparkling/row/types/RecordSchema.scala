package com.sparkling.row.types

/** An ordered sequence of [[SchemaField]]s describing the typed structure of a [[com.sparkling.row.Row]].
  *
  * This complements [[com.sparkling.schema.Fields]]:
  *   - `Fields` tracks column names only
  *   - `RecordSchema` adds runtime type and nullability for each column
  *
  * A `RecordSchema` can be converted into Spark's `StructType` for use with DataFrames and Datasets.
  */
final case class RecordSchema(columns: Vector[SchemaField]) {

  lazy val columnNames: Vector[String] = columns.map(_.name)

  def size: Int = columns.size

  def isEmpty: Boolean = columns.isEmpty

  def isSingle: Boolean = columns.size == 1

  /** Returns the [[SchemaField]] with the given name, if present. */
  def get(name: String): Option[SchemaField] = columns.find(_.name == name)

  /** Renames columns according to `mapping`, preserving order and types.
    *
    * The mapping is from old name -> new name. Columns not present in the mapping keep their name. Throws if the
    * mapping references a column name not present in this schema.
    */
  def rename(mapping: Map[String, String]): RecordSchema = {
    val known = columnNames.toSet
    val unknownKeys = mapping.keySet.diff(known)
    require(unknownKeys.isEmpty, s"rename mapping contains unknown columns: ${unknownKeys.mkString(",")}")

    val renamed = columns.map { sf =>
      mapping.get(sf.name) match {
        case None          => sf
        case Some(newName) => sf.copy(name = newName)
      }
    }

    val names = renamed.map(_.name)
    require(names.distinct.size == names.size, s"rename introduced duplicate column names: ${names.mkString(",")}")

    RecordSchema(renamed)
  }

  /** Selects a subset of columns by name, preserving original order.
    *
    * Throws if `names` contains a column not present in this schema.
    */
  def select(names: Set[String]): RecordSchema = {
    val known = columnNames.toSet
    val unknown = names.diff(known)
    require(unknown.isEmpty, s"select contains unknown columns: ${unknown.mkString(",")}")
    RecordSchema(columns.filter(sf => names.contains(sf.name)))
  }

  /** Marks all columns nullable. */
  def makeAllNullable: RecordSchema =
    RecordSchema(columns.map(sf => if (sf.nullable) sf else sf.copy(nullable = true)))
}

object RecordSchema {
  val empty: RecordSchema = RecordSchema(Vector.empty)

  def single(name: String, valueType: ValueType, nullable: Boolean = true): RecordSchema =
    RecordSchema(Vector(SchemaField(name, valueType, nullable)))
}
