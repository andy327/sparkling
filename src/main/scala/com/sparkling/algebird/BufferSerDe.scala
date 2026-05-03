package com.sparkling.algebird

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets

/** Typeclass for serializing Algebird aggregate buffers (`B`) to/from bytes.
  *
  * Spark typed aggregators (`org.apache.spark.sql.expressions.Aggregator`) require an `Encoder[B]` to shuffle
  * intermediate buffers. When `B` is not SQL-encodable (e.g. sketch or mutable structures used by Algebird),
  * `BufferSerDe` provides an explicit serialization boundary so the buffer can be stored as `Array[Byte]` — which
  * Spark can always encode.
  *
  * Instances include efficient fixed-width encodings for primitives, `Option` composition, and a Java-serialization
  * fallback for `Serializable` buffers.
  */
trait BufferSerDe[B] extends Serializable {
  def toBytes(b: B): Array[Byte]
  def fromBytes(bytes: Array[Byte]): B
}

object BufferSerDe {

  implicit val longSerDe: BufferSerDe[Long] =
    new BufferSerDe[Long] {
      override def toBytes(b: Long): Array[Byte] =
        ByteBuffer.allocate(java.lang.Long.BYTES).putLong(b).array()
      override def fromBytes(bytes: Array[Byte]): Long =
        ByteBuffer.wrap(bytes).getLong
    }

  implicit val intSerDe: BufferSerDe[Int] =
    new BufferSerDe[Int] {
      override def toBytes(b: Int): Array[Byte] =
        ByteBuffer.allocate(java.lang.Integer.BYTES).putInt(b).array()
      override def fromBytes(bytes: Array[Byte]): Int =
        ByteBuffer.wrap(bytes).getInt
    }

  implicit val doubleSerDe: BufferSerDe[Double] =
    new BufferSerDe[Double] {
      override def toBytes(b: Double): Array[Byte] =
        ByteBuffer.allocate(java.lang.Double.BYTES).putDouble(b).array()
      override def fromBytes(bytes: Array[Byte]): Double =
        ByteBuffer.wrap(bytes).getDouble
    }

  implicit val floatSerDe: BufferSerDe[Float] =
    new BufferSerDe[Float] {
      override def toBytes(b: Float): Array[Byte] =
        ByteBuffer.allocate(java.lang.Float.BYTES).putFloat(b).array()
      override def fromBytes(bytes: Array[Byte]): Float =
        ByteBuffer.wrap(bytes).getFloat
    }

  implicit val booleanSerDe: BufferSerDe[Boolean] =
    new BufferSerDe[Boolean] {
      override def toBytes(b: Boolean): Array[Byte] = Array(if (b) 1.toByte else 0.toByte)
      override def fromBytes(bytes: Array[Byte]): Boolean = bytes(0) != 0.toByte
    }

  implicit val stringSerDe: BufferSerDe[String] =
    new BufferSerDe[String] {
      override def toBytes(b: String): Array[Byte] =
        b.getBytes(StandardCharsets.UTF_8)
      override def fromBytes(bytes: Array[Byte]): String =
        new String(bytes, StandardCharsets.UTF_8)
    }

  /** Encodes `None` as a single `0` byte, and `Some(x)` as `1` followed by the payload. */
  implicit def optionSerDe[A](implicit aSerDe: BufferSerDe[A]): BufferSerDe[Option[A]] =
    new BufferSerDe[Option[A]] {
      override def toBytes(b: Option[A]): Array[Byte] =
        b match {
          case None =>
            Array(0.toByte)
          case Some(a) =>
            val payload = aSerDe.toBytes(a)
            val out = new Array[Byte](1 + payload.length)
            out(0) = 1.toByte
            System.arraycopy(payload, 0, out, 1, payload.length)
            out
        }

      override def fromBytes(bytes: Array[Byte]): Option[A] =
        if (bytes.isEmpty || bytes(0) == 0.toByte) None
        else Some(aSerDe.fromBytes(bytes.slice(1, bytes.length)))
    }

  /** Java-serialization fallback.
    *
    * Works for any `B <: java.io.Serializable`. This covers common Algebird sketch buffers such as `SpaceSaver`.
    */
  implicit def javaSerializableSerDe[B <: Serializable]: BufferSerDe[B] =
    new BufferSerDe[B] {
      override def toBytes(b: B): Array[Byte] = {
        val baos = new ByteArrayOutputStream()
        val oos = new ObjectOutputStream(baos)
        try { oos.writeObject(b); oos.flush(); baos.toByteArray }
        finally
          try oos.close()
          finally baos.close()
      }

      override def fromBytes(bytes: Array[Byte]): B = {
        val bais = new ByteArrayInputStream(bytes)
        val ois = new ObjectInputStream(bais)
        try ois.readObject().asInstanceOf[B]
        finally
          try ois.close()
          finally bais.close()
      }
    }
}
