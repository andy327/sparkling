package com.sparkling.hash

import java.nio.charset.StandardCharsets.{UTF_16, UTF_8}

import org.scalatest.matchers.should.Matchers._
import org.scalatest.wordspec.AnyWordSpec

final class HasherSpec extends AnyWordSpec {

  "primitives" should {
    "hash the same input deterministically across repeated calls" in {
      val s = "sparkling"
      val h1 = Hasher.hash(s)
      val h2 = Hasher.hash(s)
      val h3 = Hasher.hash(s)
      h1 shouldBe h2
      h2 shouldBe h3

      val i = 123456
      Hasher.hash(i) shouldBe Hasher.hash(i)

      val l = 9876543210L
      Hasher.hash(l) shouldBe Hasher.hash(l)

      Hasher.hash(true) shouldBe Hasher.hash(true)
      Hasher.hash(false) shouldBe Hasher.hash(false)
    }

    "hash different Boolean values uniquely" in {
      Hasher.hash(true) should not be Hasher.hash(false)
    }

    "hash Strings using UTF-8 and match the hashes of their UTF-8 bytes" in {
      val s1 = "hello"
      Hasher.hash(s1) shouldBe Hasher.hash(s1.getBytes(UTF_8))

      val s2 = "é—ïβ"
      Hasher.hash(s2) shouldBe Hasher.hash(s2.getBytes(UTF_8))
    }

    "treat equivalent UTF-8 String bytes the same as raw bytes, but not as other encodings" in {
      val s = "é"
      val utf8 = s.getBytes(UTF_8)
      val utf16 = s.getBytes(UTF_16) // different bytes

      Hasher.hash(s) shouldBe Hasher.hash(utf8)
      Hasher.hash(s) should not be Hasher.hash(utf16)
    }

    "not collide for the same numeric value with Ints and Longs" in {
      Hasher.hash(1) should not be Hasher.hash(1L)
      Hasher.hash(42) should not be Hasher.hash(42L)
    }

    "hash Bytes deterministically and differ across distinct values" in {
      Hasher.hash(0.toByte) shouldBe Hasher.hash(0.toByte)
      Hasher.hash(1.toByte) should not be Hasher.hash(2.toByte)
    }

    "hash Shorts deterministically and differ across distinct values" in {
      Hasher.hash(0.toShort) shouldBe Hasher.hash(0.toShort)
      Hasher.hash(100.toShort) should not be Hasher.hash(200.toShort)
    }

    "hash Floats deterministically and differ across distinct values" in {
      Hasher.hash(1.5f) shouldBe Hasher.hash(1.5f)
      Hasher.hash(1.5f) should not be Hasher.hash(2.5f)
    }

    "hash bytes deterministically and distinguish empty vs non-empty" in {
      val empty = Array.emptyByteArray
      val nonEmpty = "x".getBytes(UTF_8)

      Hasher.hash(empty) shouldBe Hasher.hash(empty)
      Hasher.hash(nonEmpty) shouldBe Hasher.hash(nonEmpty)
      Hasher.hash(empty) should not be Hasher.hash(nonEmpty)
    }
  }

  "Options" should {
    "include a presence tag so Some(x) != None and is sensitive to the payload" in {
      val noneI = Hasher.hash(Option.empty[Int])
      val someI = Hasher.hash(Option(0))
      val someI2 = Hasher.hash(Option(1))

      someI should not be noneI
      someI2 should not be someI
    }

    "produce the same hash for identical Options across calls" in {
      val o1 = Option("ABC")
      Hasher.hash(o1) shouldBe Hasher.hash(o1)

      val o2: Option[String] = None
      Hasher.hash(o2) shouldBe Hasher.hash(o2)
    }

    "differ from the raw payload hash" in {
      Hasher.hash(Option(123)) should not be Hasher.hash(123)
      Hasher.hash(Option("x")) should not be Hasher.hash("x")
      Hasher.hash(Option.empty[String]) should not be Hasher.hash("")
    }
  }

  "Ordered collections" should {
    "produce hashes for the same ordered contents regardless of Seq implementation" in {
      val list = List(1, 2, 3, 4, 5)
      val vector = Vector(1, 2, 3, 4, 5)
      Hasher.hash(list) shouldBe Hasher.hash(vector)
    }

    "produce different hashes when order changes" in {
      val xs = Seq("a", "b", "c", "d")
      val ys = Seq("d", "c", "b", "a")
      Hasher.hash(xs) should not be Hasher.hash(ys)
    }

    "match between Array and Seq with the same contents and order" in {
      val arr = Array(10L, 20L, 30L, 40L)
      val seq = Seq(10L, 20L, 30L, 40L)
      Hasher.hash(arr) shouldBe Hasher.hash(seq)
    }

    "produce the same hash for empty Arrays and empty Seqs" in {
      Hasher.hash(Array.empty[Int]) shouldBe Hasher.hash(Seq.empty[Int])
    }
  }

  "Unordered collections" should {
    "produce the same hash for different orderings" in {
      val s1 = Set(1, 2, 3, 4, 5)
      val s2 = Set(5, 4, 3, 2, 1)
      Hasher.hash(s1) shouldBe Hasher.hash(s2)
    }

    "produce different hashes for different elements" in {
      val a = Set("x", "y", "z")
      val b = Set("w", "x", "y")
      Hasher.hash(a) should not be Hasher.hash(b)
    }

    "produce different hashes than ordered collections with the same contents" in {
      val set = Set(1, 2, 3)
      val seq = Seq(1, 2, 3)
      Hasher.hash(set) should not be Hasher.hash(seq)
    }
  }

  "Maps" should {
    "be order-independent (same entries in different insertion order hash equally)" in {
      val m1 = Map("a" -> 1, "b" -> 2, "c" -> 3)
      val m2 = Map("c" -> 3, "a" -> 1, "b" -> 2)
      Hasher.hash(m1) shouldBe Hasher.hash(m2)
    }

    "differ when entries differ" in {
      val m1 = Map("a" -> 1, "b" -> 2)
      val m2 = Map("a" -> 1, "b" -> 99)
      Hasher.hash(m1) should not be Hasher.hash(m2)
    }
  }

  "Nested types" should {
    "be stable for mixed nested structures" in {
      val complex1 = Option(Seq("a", "b", "c"))
      val complex2 = Option(Vector("a", "b", "c"))

      Hasher.hash(complex1) shouldBe Hasher.hash(complex2)
    }
  }

  "Hasher[Tuple1]" should {
    "be deterministic" in {
      val a = Tuple1("alpha")
      val h1 = Hasher.hash(a)
      val h2 = Hasher.hash(a)
      h1 shouldBe h2
    }

    "change when the single component changes" in {
      Hasher.hash(Tuple1("alpha")) should not be Hasher.hash(Tuple1("beta"))
      Hasher.hash(Tuple1(1)) should not be Hasher.hash(Tuple1(2))
    }

    "distinguish None vs Some in Option payload" in {
      Hasher.hash(Tuple1(None: Option[Int])) should not be Hasher.hash(Tuple1(Option(0)))
      Hasher.hash(Tuple1(Option("x"))) should not be Hasher.hash(Tuple1(Option("y")))
    }
  }

  "Hasher[(A,B)]" should {
    "be deterministic" in {
      val t = ("alpha", 1)
      Hasher.hash(t) shouldBe Hasher.hash(t)
    }

    "be order-sensitive" in {
      Hasher.hash(("alpha", 1)) should not be Hasher.hash((1, "alpha"))
    }

    "reflect changes in either component" in {
      val base = Hasher.hash(("alpha", 1))
      Hasher.hash(("alpha", 2)) should not be base
      Hasher.hash(("beta", 1)) should not be base
    }
  }

  "Hasher[(A,B,C)]" should {
    "be deterministic" in {
      val t = ("a", 1, true)
      Hasher.hash(t) shouldBe Hasher.hash(t)
    }

    "be order-sensitive" in {
      Hasher.hash(("a", 1, true)) should not be Hasher.hash((1, "a", true))
      Hasher.hash(("a", 1, true)) should not be Hasher.hash(("a", true, 1))
    }

    "reflect changes in any component" in {
      val base = Hasher.hash(("a", 1, true))
      Hasher.hash(("a", 2, true)) should not be base
      Hasher.hash(("b", 1, true)) should not be base
      Hasher.hash(("a", 1, false)) should not be base
    }
  }

  "Hasher[(A,B,C,D)]" should {
    "be deterministic" in {
      val t = ("a", 1, true, 2.0)
      Hasher.hash(t) shouldBe Hasher.hash(t)
    }

    "be order-sensitive" in {
      Hasher.hash(("a", 1, true, 2.0)) should not be Hasher.hash((2.0, 1, true, "a"))
    }

    "reflect changes in any component" in {
      val base = Hasher.hash(("a", 1, true, 2.0))
      Hasher.hash(("a", 1, true, 3.0)) should not be base
      Hasher.hash(("a", 1, false, 2.0)) should not be base
    }
  }

  "Hasher[(A,B,C,D,E)]" should {
    "be deterministic" in {
      val t = ("a", 1, true, 2.0, 'z')
      Hasher.hash(t) shouldBe Hasher.hash(t)
    }

    "be order-sensitive" in {
      val t1 = ("a", 1, true, 2.0, 'z')
      val t2 = (1, true, 2.0, 'z', "a")
      Hasher.hash(t1) should not be Hasher.hash(t2)
    }

    "handle Options inside larger tuples" in {
      val tA = ("a", Option.empty[Int], true, 2.0, 'z')
      val tB = ("a", Some(0): Option[Int], true, 2.0, 'z')
      Hasher.hash(tA) should not be Hasher.hash(tB)
    }
  }
}
