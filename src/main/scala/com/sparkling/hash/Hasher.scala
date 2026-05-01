package com.sparkling.hash

import net.openhft.hashing.LongHashFunction
import shapeless.{::, Generic, HList, HNil}

/** Hashing typeclass (single-abstract-method) that encodes values into a 64-bit Long hash.
  *
  * Supported types & semantics
  *   - Primitives: `String` (UTF-8), `Int`, `Long`, `Boolean`, `Array[Byte]`
  *   - Options: presence tag + payload (if present)
  *   - Ordered collections: `Seq[A]`, `Array[A]` — order matters
  *   - Unordered collections: `Set[A]` — order does not matter
  *   - Maps: `Map[K, V]` — key order does not matter
  *   - Products (tuples, case classes): derived via shapeless; hashed as an ordered sequence of element hashes
  *
  * Results are deterministic across platforms that implement XXHash64.
  */
trait Hasher[A] extends Serializable {
  def hash(value: A): Long
}

trait HasherLowPriority extends Serializable {
  // Covers any concrete Seq subtype (List, Vector, Queue, Stream, etc.) by upcasting to Seq[A] and
  // delegating to seqHasher. This is in a low-priority trait so that the direct Seq[A] instance takes
  // precedence when the static type is already Seq[A].
  implicit def seqSubtypeHasher[C[X] <: Seq[X], A](implicit ha: Hasher[A], ev: C[A] <:< Seq[A]): Hasher[C[A]] =
    (xs: C[A]) => Hasher.seqHasher[A].hash(ev(xs))
}

/** Default Hasher instances backed by a fast 64-bit hash (XXHash64).
  *
  * Products (tuples, case classes) are derived generically (shapeless) and folded with the same ordered rolling mixer
  * used for `Seq`/`Array`, so `(a, b, c)` hashes the same as `Seq(a, b, c).map(hash)...`.
  */
object Hasher extends HasherLowPriority {

  /** Computes a 64-bit hash for any type A with an implicit Hasher[A]. */
  def hash[A](x: A)(implicit hasher: Hasher[A]): Long = hasher.hash(x)

  // XXHash64 is a fast, non-cryptographic hashing algorithm that produces a 64-bit hash value
  private val XX: LongHashFunction = LongHashFunction.xx()

  // $COVERAGE-OFF$
  // presence tags for Some and None
  final private val TagSome = 0xa1L
  final private val TagNone = 0xa0L

  // mixing constants for good diffusion
  final private val Golden1 = 0x9e3779b97f4a7c15L // 64-bit golden ratio constant
  final private val Golden2 = 0x9ddfea08eb382d69L // wyhash multiplier

  // nonzero initial entropy constant for rolling hash
  final private val Seed = 0x165667b19e3779f9L
  // $COVERAGE-ON$

  implicit val byteHasher: Hasher[Byte] =
    (b: Byte) => XX.hashInt(b.toInt)

  implicit val shortHasher: Hasher[Short] =
    (s: Short) => XX.hashInt(s.toInt)

  implicit val intHasher: Hasher[Int] =
    (i: Int) => XX.hashInt(i)

  implicit val longHasher: Hasher[Long] =
    (l: Long) => XX.hashLong(l)

  implicit val floatHasher: Hasher[Float] =
    (f: Float) => XX.hashLong(java.lang.Float.floatToIntBits(f).toLong)

  implicit val doubleHasher: Hasher[Double] =
    (d: Double) => XX.hashLong(java.lang.Double.doubleToLongBits(d))

  implicit val charHasher: Hasher[Char] =
    (c: Char) => XX.hashInt(c.toInt)

  implicit val booleanHasher: Hasher[Boolean] =
    (b: Boolean) => XX.hashLong(if (b) 1L else 0L)

  implicit val stringHasher: Hasher[String] =
    (s: String) => XX.hashBytes(s.getBytes(java.nio.charset.StandardCharsets.UTF_8))

  implicit val bytesHasher: Hasher[Array[Byte]] =
    (b: Array[Byte]) => XX.hashBytes(b)

  implicit def optionHasher[A](implicit H: Hasher[A]): Hasher[Option[A]] =
    (opt: Option[A]) =>
      opt match {
        case Some(a) => finalize64(TagSome ^ H.hash(a))
        case None    => finalize64(TagNone)
      }

  implicit def seqHasher[A](implicit H: Hasher[A]): Hasher[Seq[A]] = {
    case xs: IndexedSeq[A] =>
      var i = 0
      val n = xs.length
      combineOrderedHashes(() => { val h = H.hash(xs(i)); i += 1; h }, () => i < n)
    case xs =>
      combineOrderedIterator(xs.iterator)(H.hash)
  }

  implicit def arrayHasher[A](implicit H: Hasher[A]): Hasher[Array[A]] =
    (xs: Array[A]) => combineOrderedArray(xs)(H.hash)

  implicit def setHasher[A](implicit H: Hasher[A]): Hasher[Set[A]] =
    (xs: Set[A]) => {
      var acc = UnorderedAccumulator.zero
      val it = xs.iterator
      while (it.hasNext)
        acc = acc.add(H.hash(it.next()))
      acc.finish()
    }

  implicit def mapHasher[K, V](implicit H: Hasher[Set[(K, V)]]): Hasher[Map[K, V]] =
    (kvs: Map[K, V]) => H.hash(kvs.toSet)

  /** Acts as a final mixing step; ensures that small input changes affect all bits of the output. */
  @inline private def finalize64(x0: Long): Long = {
    var x = x0
    x ^= x >>> 33; x *= 0xff51afd7ed558ccdL
    x ^= x >>> 33; x *= 0xc4ceb9fe1a85ec53L
    x ^= x >>> 33
    x
  }

  /** Performs a bitwise rotation to the left by k bits over a 64-bit integer. */
  @inline private def rotateLeft64(x: Long, k: Int): Long = {
    val n = k & 63
    (x << n) | (x >>> (64 - n))
  }

  /** Folds a sequence of 64-bit element hashes into a single ordered hash value using an ordered rolling mix.
    *
    * This helper method can be used for combining typed elements that exist in an Array or are unpacked from an
    * Iterator of hashes.
    */
  @inline private def combineOrderedHashes(next: () => Long, hasNext: () => Boolean): Long = {
    var h = Seed // running hash accumulator
    var i = 0L // index of current element
    while (hasNext()) {
      val e = finalize64(next()) // hash of current element
      h = finalize64((h ^ (e + i * Golden1)) * Golden2)
      i += 1L
    }
    finalize64(h ^ (i * Golden1))
  }

  /** Combines an Iterator of typed elements into a single hash. */
  private def combineOrderedIterator[A](it: Iterator[A])(encode: A => Long): Long =
    combineOrderedHashes(() => encode(it.next()), () => it.hasNext)

  /** Combines an Array of typed elements into a single hash. */
  private def combineOrderedArray[A](xs: Array[A])(encode: A => Long): Long = {
    var index = 0
    val size = xs.length
    combineOrderedHashes(
      next = () => {
        val h = encode(xs(index))
        index += 1
        h
      },
      hasNext = () => index < size
    )
  }

  /** Commutative/associative accumulator for unordered sets.
    *
    * We combine multiple symmetric aggregates (sum/xor/sumSquares) and finalize. This avoids sorting (O(n)) and
    * preserves order-independence.
    */
  @inline final private case class UnorderedAccumulator(sum: Long, xor: Long, sum2: Long, count: Int) {
    @inline def add(x: Long): UnorderedAccumulator = {
      val z = finalize64(x)
      UnorderedAccumulator(
        sum = sum + z,
        xor = xor ^ rotateLeft64(z, 17),
        sum2 = sum2 + z * z, // overflow modulo 64-bit is fine for hashing
        count = count + 1
      )
    }

    @inline def finish(): Long = finalize64(sum ^ xor ^ (sum2 * Golden1) ^ (count.toLong * Golden2))
  }

  private object UnorderedAccumulator {
    val zero: UnorderedAccumulator = UnorderedAccumulator(0L, 0L, 0L, 0)
  }

  // shapeless is used for deriving product-based hashes (tuples and case classes)

  /** Internal helper: folds the ordered rolling hash directly over an HList of values.
    *
    * `fold` takes the current running hash `h` and element index `i`, incorporates the next field's hash using the same
    * mixing step as [[combineOrderedHashes]], and recurses into the tail. `hnil` applies the final mix step, mirroring
    * the loop epilogue in [[combineOrderedHashes]].
    */
  private[hash] trait FoldHash[L <: HList] extends Serializable {
    def fold(l: L, h: Long, i: Long): Long
  }

  object FoldHash {
    implicit val hnil: FoldHash[HNil] =
      (_: HNil, h: Long, i: Long) => finalize64(h ^ (i * Golden1))

    implicit def hcons[H, T <: HList](implicit hh: Hasher[H], th: FoldHash[T]): FoldHash[H :: T] =
      (l: H :: T, h: Long, i: Long) => {
        val e = finalize64(hh.hash(l.head))
        val h2 = finalize64((h ^ (e + i * Golden1)) * Golden2)
        th.fold(l.tail, h2, i + 1L)
      }
  }

  /** Hash any tuple or case class `A` by folding the ordered rolling hash directly over its HList representation, with
    * no intermediate buffer or array allocation.
    */
  implicit def productHasher[A, L <: HList](implicit gen: Generic.Aux[A, L], fh: FoldHash[L]): Hasher[A] =
    (a: A) => fh.fold(gen.to(a), Seed, 0L)
}
