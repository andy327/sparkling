package com.sparkling.row.codec

import scala.reflect.ClassTag

import com.sparkling.row.Row
import com.sparkling.row.types.{RecordSchema, ValueType}

/** Typeclass describing how to encode and decode a single cell value `A` to/from a runtime representation.
  *
  * `ValueCodec` is the single source of truth for cell-level codec behaviour. It provides:
  *   - a runtime [[com.sparkling.row.types.ValueType]] tag for `A`
  *   - a [[nullable]] flag indicating whether this cell may legally hold `null`
  *   - [[encode]]: `A => Any` for writing values into rows (hot-path safe, no allocation)
  *   - [[decodeUnsafe]]: `Any => A` for reading values on the hot path (throws on failure, no `Either` allocation)
  *   - [[decode]]: `Any => Either[Throwable, A]` for safe decoding where callers need error recovery (used by
  *     [[RecordCodec]] on non-hot paths)
  *
  * == Null policy ==
  * Primitive codecs (`Int`, `Long`, `Double`, `Boolean`) treat `null` as an error in both `decode` and `decodeUnsafe`.
  * Use `Option[A]` for nullable primitives. `String` follows Spark-style semantics and passes `null` through.
  * Collection and map codecs also pass `null` through.
  *
  * == Coercion ==
  * Best-effort numeric coercions are supported (e.g. `Long -> Int`, `Int -> Long`, `String -> Double`). Boolean does
  * not coerce from `String` to avoid silent mismatches in row-level pipelines.
  *
  * == Custom types ==
  * For types not covered by the built-in instances, implement `ValueCodec[T]` directly. A `RowDecoder[T]` and
  * `RowEncoder[T]` are automatically derived from any `ValueCodec[T]` in scope.
  *
  * == Nested rows ==
  * [[ValueCodec.forRow]] produces a `ValueCodec[Row]` for object-typed cells. This is not provided implicitly because
  * it requires a runtime schema.
  */
trait ValueCodec[A] extends Serializable {

  /** Runtime type of `A`. */
  def valueType: ValueType

  /** Whether this cell may legally be null in row encodings.
    *
    * `false` for primitives (`Int`, `Long`, `Double`, `Boolean`); `true` for `String`, `Option`, collections,
    * maps, and nested rows.
    */
  def nullable: Boolean

  /** Encode `a` to an engine-neutral cell representation. Never throws. */
  def encode(a: A): Any

  /** Decodes a cell value to `A`, throwing on failure.
    *
    * Used on hot paths (e.g. per-row inside Spark UDFs) where `Either` allocation would be costly. Callers on hot paths
    * should guard null/missing values with [[com.sparkling.row.RowUtil.allPresent]] before calling this method.
    */
  def decodeUnsafe(any: Any): A

  /** Decodes a cell value to `A`, returning `Left` on failure.
    *
    * Used by [[RecordCodec]] and other non-hot-path callers that need to accumulate or recover from errors. The default
    * implementation wraps [[decodeUnsafe]] in a try/catch. Primitive codecs (`Int`, `Long`, etc.) override this to
    * avoid the try/catch overhead on the happy path and to produce more specific `Left` types.
    */
  def decode(any: Any): Either[Throwable, A] =
    try Right(decodeUnsafe(any))
    catch { case e: Throwable => Left(e) }
}

object ValueCodec {
  import ValueType._

  // primitives

  implicit val string: ValueCodec[String] = new ValueCodec[String] {
    val valueType: ValueType = StringType
    val nullable: Boolean = true
    def encode(a: String): Any = a
    def decodeUnsafe(any: Any): String =
      any match {
        case null      => null
        case s: String => s
        case x         => throw new ClassCastException(s"Cannot decode String from value=$x (${x.getClass.getName})")
      }
  }

  implicit val int: ValueCodec[Int] = new ValueCodec[Int] {
    val valueType: ValueType = IntType
    val nullable: Boolean = false
    def encode(a: Int): Any = a
    def decodeUnsafe(any: Any): Int =
      any match {
        case null      => throw new NoSuchElementException("Int value is null (use Option[Int] for nullable fields)")
        case i: Int    => i
        case l: Long   => l.toInt
        case s: String => s.toInt
        case x         => throw new ClassCastException(s"Cannot decode Int from value=$x (${x.getClass.getName})")
      }
    // override decode to propagate NumberFormatException as Left rather than wrapping in a try/catch
    override def decode(any: Any): Either[Throwable, Int] =
      any match {
        case null      => Left(new NoSuchElementException("Int value is null (use Option[Int] for nullable fields)"))
        case i: Int    => Right(i)
        case l: Long   => Right(l.toInt)
        case s: String =>
          try Right(s.toInt)
          catch { case e: NumberFormatException => Left(e) }
        case x => Left(new ClassCastException(s"Cannot decode Int from value=$x (${x.getClass.getName})"))
      }
  }

  implicit val long: ValueCodec[Long] = new ValueCodec[Long] {
    val valueType: ValueType = LongType
    val nullable: Boolean = false
    def encode(a: Long): Any = a
    def decodeUnsafe(any: Any): Long =
      any match {
        case null      => throw new NoSuchElementException("Long value is null (use Option[Long] for nullable fields)")
        case l: Long   => l
        case i: Int    => i.toLong
        case s: String => s.toLong
        case x         => throw new ClassCastException(s"Cannot decode Long from value=$x (${x.getClass.getName})")
      }
    override def decode(any: Any): Either[Throwable, Long] =
      any match {
        case null      => Left(new NoSuchElementException("Long value is null (use Option[Long] for nullable fields)"))
        case l: Long   => Right(l)
        case i: Int    => Right(i.toLong)
        case s: String =>
          try Right(s.toLong)
          catch { case e: NumberFormatException => Left(e) }
        case x => Left(new ClassCastException(s"Cannot decode Long from value=$x (${x.getClass.getName})"))
      }
  }

  implicit val double: ValueCodec[Double] = new ValueCodec[Double] {
    val valueType: ValueType = DoubleType
    val nullable: Boolean = false
    def encode(a: Double): Any = a
    def decodeUnsafe(any: Any): Double =
      any match {
        case null => throw new NoSuchElementException("Double value is null (use Option[Double] for nullable fields)")
        case d: Double => d
        case f: Float  => f.toDouble
        case l: Long   => l.toDouble
        case i: Int    => i.toDouble
        case s: String => s.toDouble
        case x         => throw new ClassCastException(s"Cannot decode Double from value=$x (${x.getClass.getName})")
      }
    override def decode(any: Any): Either[Throwable, Double] =
      any match {
        case null => Left(new NoSuchElementException("Double value is null (use Option[Double] for nullable fields)"))
        case d: Double => Right(d)
        case f: Float  => Right(f.toDouble)
        case l: Long   => Right(l.toDouble)
        case i: Int    => Right(i.toDouble)
        case s: String =>
          try Right(s.toDouble)
          catch { case e: NumberFormatException => Left(e) }
        case x => Left(new ClassCastException(s"Cannot decode Double from value=$x (${x.getClass.getName})"))
      }
  }

  // Boolean intentionally does not coerce from String to avoid silent mismatches in row pipelines.
  implicit val boolean: ValueCodec[Boolean] = new ValueCodec[Boolean] {
    val valueType: ValueType = BooleanType
    val nullable: Boolean = false
    def encode(a: Boolean): Any = a
    def decodeUnsafe(any: Any): Boolean =
      any match {
        case null => throw new NoSuchElementException("Boolean value is null (use Option[Boolean] for nullable fields)")
        case b: Boolean => b
        case x          => throw new ClassCastException(s"Cannot decode Boolean from value=$x (${x.getClass.getName})")
      }
  }

  // Option

  implicit def option[A](implicit vc: ValueCodec[A]): ValueCodec[Option[A]] =
    new ValueCodec[Option[A]] {
      val valueType: ValueType = vc.valueType
      val nullable: Boolean = true
      def encode(a: Option[A]): Any =
        if (a == null) null
        else
          a match {
            case None    => null
            case Some(x) => vc.encode(x)
          }
      def decodeUnsafe(any: Any): Option[A] =
        if (any == null) None else Some(vc.decodeUnsafe(any))
      override def decode(any: Any): Either[Throwable, Option[A]] =
        if (any == null) Right(None) else vc.decode(any).map(Some(_))
    }

  // Collections
  //
  // All collection decoders accept Iterable and Array inputs. This handles the full range of runtime representations
  // that may appear in rows: Scala collections, Spark's internal array representations, and values already in the
  // target collection type (e.g. Set stored directly in a row cell). Iterable is checked before Array since most Scala
  // collections match Iterable.

  implicit def seq[A](implicit vc: ValueCodec[A]): ValueCodec[Seq[A]] =
    new ValueCodec[Seq[A]] {
      val valueType: ValueType = ArrayType(vc.valueType)
      val nullable: Boolean = true
      def encode(as: Seq[A]): Any =
        if (as == null) null else as.iterator.map(vc.encode).toVector
      def decodeUnsafe(any: Any): Seq[A] =
        any match {
          case null            => throw new NoSuchElementException("Seq value is null")
          case it: Iterable[_] => it.iterator.map(vc.decodeUnsafe).toVector
          case arr: Array[_]   => arr.iterator.map(vc.decodeUnsafe).toVector
          case x => throw new ClassCastException(s"Cannot decode Seq from value=$x (${x.getClass.getName})")
        }
    }

  implicit def array[A: ClassTag](implicit vc: ValueCodec[A]): ValueCodec[Array[A]] =
    new ValueCodec[Array[A]] {
      val valueType: ValueType = ArrayType(vc.valueType)
      val nullable: Boolean = true
      def encode(as: Array[A]): Any =
        if (as == null) null else as.iterator.map(vc.encode).toVector
      def decodeUnsafe(any: Any): Array[A] =
        any match {
          case null            => throw new NoSuchElementException("Array value is null")
          case it: Iterable[_] => it.iterator.map(vc.decodeUnsafe).toArray
          case arr: Array[_]   => arr.iterator.map(vc.decodeUnsafe).toArray
          case x => throw new ClassCastException(s"Cannot decode Array from value=$x (${x.getClass.getName})")
        }
    }

  implicit def vector[A](implicit vc: ValueCodec[A]): ValueCodec[Vector[A]] =
    new ValueCodec[Vector[A]] {
      val valueType: ValueType = ArrayType(vc.valueType)
      val nullable: Boolean = true
      def encode(as: Vector[A]): Any =
        if (as == null) null else as.map(vc.encode)
      def decodeUnsafe(any: Any): Vector[A] =
        any match {
          case null            => throw new NoSuchElementException("Vector value is null")
          case it: Iterable[_] => it.iterator.map(vc.decodeUnsafe).toVector
          case arr: Array[_]   => arr.iterator.map(vc.decodeUnsafe).toVector
          case x => throw new ClassCastException(s"Cannot decode Vector from value=$x (${x.getClass.getName})")
        }
    }

  implicit def set[A](implicit vc: ValueCodec[A]): ValueCodec[Set[A]] =
    new ValueCodec[Set[A]] {
      val valueType: ValueType = ArrayType(vc.valueType)
      val nullable: Boolean = true
      def encode(as: Set[A]): Any =
        if (as == null) null else as.iterator.map(vc.encode).toVector
      def decodeUnsafe(any: Any): Set[A] =
        any match {
          case null            => throw new NoSuchElementException("Set value is null")
          case it: Iterable[_] => it.iterator.map(vc.decodeUnsafe).toSet
          case arr: Array[_]   => arr.iterator.map(vc.decodeUnsafe).toSet
          case x => throw new ClassCastException(s"Cannot decode Set from value=$x (${x.getClass.getName})")
        }
    }

  // Map

  implicit def map[K, V](implicit kc: ValueCodec[K], vc: ValueCodec[V]): ValueCodec[Map[K, V]] =
    new ValueCodec[Map[K, V]] {
      val valueType: ValueType = MapType(kc.valueType, vc.valueType)
      val nullable: Boolean = true
      def encode(as: Map[K, V]): Any =
        if (as == null) null
        else as.iterator.map { case (k, v) => kc.encode(k) -> vc.encode(v) }.toMap
      def decodeUnsafe(any: Any): Map[K, V] =
        any match {
          case null =>
            throw new NoSuchElementException("Map value is null")
          case m: scala.collection.Map[_, _] =>
            m.iterator.map { case (k, v) => kc.decodeUnsafe(k) -> vc.decodeUnsafe(v) }.toMap
          case jm: java.util.Map[_, _] =>
            import scala.jdk.CollectionConverters._
            jm.asScala.iterator.map { case (k, v) => kc.decodeUnsafe(k) -> vc.decodeUnsafe(v) }.toMap
          case x =>
            throw new ClassCastException(s"Cannot decode Map from value=$x (${x.getClass.getName})")
        }
    }

  // Nested row

  /** Produces a `ValueCodec[Row]` for object-typed (nested struct) cells.
    *
    * Not provided implicitly because it requires a runtime [[com.sparkling.row.types.RecordSchema]].
    */
  def forRow(schema: RecordSchema): ValueCodec[Row] =
    new ValueCodec[Row] {
      val valueType: ValueType = ValueType.ObjectType(schema)
      val nullable: Boolean = true
      def encode(r: Row): Any = r
      def decodeUnsafe(any: Any): Row =
        any match {
          case null   => throw new NoSuchElementException("Nested Row value is null")
          case r: Row => r
          case x      => throw new ClassCastException(s"Cannot decode Row from value=$x (${x.getClass.getName})")
        }
    }
}
