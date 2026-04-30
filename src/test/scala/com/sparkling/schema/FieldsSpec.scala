package com.sparkling.schema

import org.scalatest.matchers.should.Matchers._
import org.scalatest.wordspec.AnyWordSpec

final class FieldsSpec extends AnyWordSpec {

  "Fields" should {
    "preserve exact names and order and expose basic operations" in {
      val f = Fields("a", "b", "c")
      f.size shouldBe 3
      f.isEmpty shouldBe false
      f.nonEmpty shouldBe true
      f.contains("b") shouldBe true
      f.contains("z") shouldBe false
      f.toVector shouldBe Vector("a", "b", "c")
      f.toString shouldBe "[a,b,c]"
    }

    "expose the internal array for hot paths (toArrayUnsafe) without copying" in {
      val raw = Array("x", "y")
      val fs = Fields.fromArray(raw)

      val arr = fs.toArrayUnsafe
      arr shouldBe theSameInstanceAs(raw)
    }

    "construct via fromSeq equivalently to varargs" in {
      val a = Fields("x", "y")
      val b = Fields.fromSeq(Seq("x", "y"))
      a shouldBe b
      a.hashCode() shouldBe b.hashCode()
    }

    "support empty Fields (zero names)" in {
      val e = Fields.empty
      e.size shouldBe 0
      e.isEmpty shouldBe true
      e.nonEmpty shouldBe false
      e.names shouldBe Vector.empty
      e.prefix("p_") shouldBe Fields.empty
      e.postfix("_s") shouldBe Fields.empty
      e.toString shouldBe "[]"
    }

    "support concatenation (++) with validation" in {
      val f1 = Fields("a", "b")
      val f2 = Fields("c")
      val f = f1 ++ f2
      f.toVector shouldBe Vector("a", "b", "c")
    }

    "fail concatenation when it introduces duplicates" in {
      val f1 = Fields("a", "b")
      val f2 = Fields("b")
      an[IllegalArgumentException] shouldBe thrownBy(f1 ++ f2)
    }

    "provide containsName and ordinalOf helpers (including missing-name behavior)" in {
      val f = Fields("u", "v", "w")

      f.containsName("u") shouldBe true
      f.containsName("zzz") shouldBe false

      f.ordinalOf("v") shouldBe 1
      an[NoSuchElementException] shouldBe thrownBy(f.ordinalOf("zzz"))
    }

    "compute ordinalsIn and throw when a requested name is missing in the target schema" in {
      val requested = Fields("a", "c")
      val schema = Fields("a", "b", "c")

      requested.ordinalsIn(schema) shouldBe Array(0, 2)

      val missing = Fields("a", "zzz")
      an[NoSuchElementException] shouldBe thrownBy(missing.ordinalsIn(schema))
    }

    "implement equals defensively for different sizes, differing content, and non-Fields types" in {
      (Fields("a") should not).equal(Fields("a", "b"))
      (Fields("a", "b") should not).equal(Fields("a", "c"))
      Fields("a").equals("not-fields") shouldBe false
    }

    "support :+ append with validation for raw name and Field" in {
      val f = Fields("a") :+ "b"
      f.toVector shouldBe Vector("a", "b")
      an[IllegalArgumentException] shouldBe thrownBy(Fields("a") :+ "a")

      val g = Fields("x") :+ Field("y")
      g.toVector shouldBe Vector("x", "y")
      an[IllegalArgumentException] shouldBe thrownBy(Fields("x") :+ Field("x"))
    }

    "discard named fields, ignoring fields not present" in {
      val f = Fields("a", "b", "c", "d")

      f.discard(Fields("b", "d")) shouldBe Fields("a", "c")
      f.discard(Fields("a")) shouldBe Fields("b", "c", "d")

      // fields not present are silently ignored
      f.discard(Fields("b", "zzz")) shouldBe Fields("a", "c", "d")

      // discarding nothing returns all fields
      f.discard(Fields.empty) shouldBe f

      // discarding all returns empty
      f.discard(Fields("a", "b", "c", "d")) shouldBe Fields.empty
    }

    "keep only named fields, ignoring fields not present, preserving order from this set" in {
      val f = Fields("a", "b", "c", "d")

      f.keep(Fields("b", "d")) shouldBe Fields("b", "d")
      f.keep(Fields("a", "c")) shouldBe Fields("a", "c")

      // order follows this set, not the argument
      f.keep(Fields("d", "a")) shouldBe Fields("a", "d")

      // fields not present in this set are silently ignored
      f.keep(Fields("b", "zzz")) shouldBe Fields("b")

      // keeping nothing returns empty
      f.keep(Fields.empty) shouldBe Fields.empty

      // keeping all returns all
      f.keep(Fields("a", "b", "c", "d")) shouldBe f
    }

    "prefix and postfix all names" in {
      val f = Fields("a", "b").prefix("pre_")
      f.names shouldBe Vector("pre_a", "pre_b")

      val g = Fields("a", "b").postfix("_suf")
      g.names shouldBe Vector("a_suf", "b_suf")
    }

    "reject null names" in {
      an[IllegalArgumentException] shouldBe thrownBy(Fields(null, "b"))
      an[IllegalArgumentException] shouldBe thrownBy(Fields.fromSeq(Seq("a", null)))
    }

    "reject empty names" in {
      an[IllegalArgumentException] shouldBe thrownBy(Fields(""))
      an[IllegalArgumentException] shouldBe thrownBy(Fields.fromSeq(Seq("")))
    }

    "reject leading or trailing whitespace" in {
      an[IllegalArgumentException] shouldBe thrownBy(Fields("  a"))
      an[IllegalArgumentException] shouldBe thrownBy(Fields("a  "))
      an[IllegalArgumentException] shouldBe thrownBy(Fields(" a "))
    }

    "reject duplicate names" in {
      an[IllegalArgumentException] shouldBe thrownBy(Fields("a", "b", "a"))
    }

    "create multiple temporary fields via tmp" in {
      val fs = Fields.tmp(3, "step-")
      fs.size shouldBe 3
      fs.names.foreach { name =>
        name should startWith("step-")
        val suf = name.stripPrefix("step-")
        suf.length shouldBe 16
        (suf should fullyMatch).regex("^[a-f0-9]+$")
      }
      fs.names.distinct.size shouldBe 3
    }

    "wrap a single name with one(Field)" in {
      Fields.one(Field("id")).names shouldBe Vector("id")
    }

    "return the index of a field name" in {
      val f = Fields("u", "v", "w")
      f.indexOf("u") shouldBe 0
      f.indexOf("w") shouldBe 2
      f.indexOf("zzz") shouldBe -1
    }

    "check whether it contains all the fields in a Fields object" in {
      val f = Fields("u", "v", "w")
      f.containsAll(Fields("u")) shouldBe true
      f.containsAll(Fields("u", "w")) shouldBe true
      f.containsAll(Fields("u", "zzz")) shouldBe false
    }

    "merge Fields while preserving first occurrence and order" in {
      val a = Fields("a", "b")
      val b = Fields("b", "c")
      val c = Fields("a", "d")
      val merged = Fields.merge(a, b, c)
      merged.names shouldBe Vector("a", "b", "c", "d")
    }

    "handle merging empty Fields and zero arguments" in {
      Fields.merge() shouldBe Fields.empty
      Fields.merge(Fields.empty, Fields("x")) shouldBe Fields("x")
      Fields.merge(Fields("x"), Fields.empty) shouldBe Fields("x")
    }

    "calculate the intersection of multiple Fields objects while preserving order from the first set" in {
      val a = Fields("a", "b", "c", "d")
      val b = Fields("c", "a", "x")
      val c = Fields("d", "a", "c", "y")
      val inter = Fields.intersect(a, b, c)
      inter.names shouldBe Vector("a", "c")
    }

    "handle calculating the intersection with zero or one arguments" in {
      Fields.intersect() shouldBe Fields.empty
      val single = Fields("p", "q")
      Fields.intersect(single) shouldBe single
    }

    "return an empty intersection when Fields are disjoint" in {
      val a = Fields("a", "b")
      val b = Fields("x", "y")
      Fields.intersect(a, b) shouldBe Fields.empty
    }
  }
}
