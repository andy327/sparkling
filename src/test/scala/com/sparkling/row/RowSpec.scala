package com.sparkling.row

import org.scalatest.matchers.should.Matchers._
import org.scalatest.wordspec.AnyWordSpec

import com.sparkling.schema.{Field, Fields}

final class RowSpec extends AnyWordSpec {

  "Row" should {
    "build from a Map and expose values via get/getAs/contains" in {
      val row = Row.fromMap(Map("a" -> 1, "b" -> "x", "名" -> 42))

      // schema should be sorted by key name
      row.schema.names.toSeq shouldBe Seq("a", "b", "名")

      row.get("a") shouldBe Some(1)
      row.getAs[Int]("a") shouldBe Some(1)
      row.getAs[String]("b") shouldBe Some("x")
      row.getAs[Int]("名") shouldBe Some(42)

      row.contains("a") shouldBe true
      row.contains("名") shouldBe true
      row.contains("missing") shouldBe false
    }

    "expose arity and ordinal access, and support fromSeq" in {
      val schema = Fields("a", "b", "c")

      val row1 = Row.fromArray(schema, Array[Any](10, "hi", true))
      row1.arity shouldBe 3
      row1.getAt(1) shouldBe "hi"

      val row2 = Row.fromSeq(schema, Seq[Any](10, "hi", true))
      row2.schema shouldBe schema
      row2.values.toSeq shouldBe Seq(10, "hi", true)
      row2.getAt(2) shouldBe true
    }

    "treat null and missing keys from Map as absent" in {
      val row = Row.fromMap(Map("a" -> null))

      row.get("a") shouldBe None
      row.get("missing") shouldBe None

      row.contains("a") shouldBe false
      row.contains("missing") shouldBe false
    }

    "throw an exception on missing or null values with applyUnsafe" in {
      val row = Row.fromMap(Map("a" -> null))

      an[NoSuchElementException] shouldBe thrownBy(row.applyUnsafe("a"))
      an[NoSuchElementException] shouldBe thrownBy(row.applyUnsafe("missing"))
    }

    "have a readable toString for Map-backed rows" in {
      val row = Row.fromMap(Map("z" -> 1))
      val s = row.toString

      s should include("Row(")
      s should include("schema=")
      s should include("z") // schema should mention the field name
    }

    "build from a schema and values, exposing them via get/getAs/contains" in {
      val schema = Fields("a", "b", "c")
      val row = Row.fromArray(schema, Array(10, "hi", true))

      row.schema shouldBe schema
      row.values.toSeq shouldBe Seq(10, "hi", true)

      row.getAs[Int]("a") shouldBe Some(10)
      row.getAs[String]("b") shouldBe Some("hi")
      row.getAs[Boolean]("c") shouldBe Some(true)

      row.contains("a") shouldBe true
      row.contains("b") shouldBe true
      row.contains("c") shouldBe true
    }

    "treat null and missing fields as absent for schema-backed rows" in {
      val schema = Fields("a", "b")
      val row = Row.fromArray(schema, Array(null, 5))

      row.get("a") shouldBe None
      row.get("missing") shouldBe None

      row.contains("a") shouldBe false
      row.contains("missing") shouldBe false
    }

    "throw an exception if schema and values lengths differ" in {
      val schema = Fields("a", "b", "c")

      an[IllegalArgumentException] shouldBe thrownBy(Row.fromArray(schema, Array(1, 2)))
    }

    "have a readable toString for schema-backed rows" in {
      val schema = Fields("x")
      val row = Row.fromArray(schema, Array(1))
      val s = row.toString

      s should include("Row(schema=")
      s should include("x")
    }

    "append extra fields and values via withValues" in {
      val baseSchema = Fields("a")
      val baseRow = Row.fromArray(baseSchema, Array(1))

      val extraFields = Fields("b", "c")
      val extraValues: Array[Any] = Array(2, "x")

      val extended = baseRow.withValues(extraFields, extraValues)

      extended.schema.names.toSeq shouldBe Seq("a", "b", "c")
      extended.values.toSeq shouldBe Seq(1, 2, "x")

      extended.getAs[Int]("a") shouldBe Some(1)
      extended.getAs[Int]("b") shouldBe Some(2)
      extended.getAs[String]("c") shouldBe Some("x")
    }

    "fail withValues when field and value counts differ" in {
      val baseSchema = Fields("a")
      val baseRow = Row.fromArray(baseSchema, Array(1))

      val extraFields = Fields("b", "c")
      val extraValues: Array[Any] = Array(2) // length mismatch

      an[IllegalArgumentException] shouldBe thrownBy(baseRow.withValues(extraFields, extraValues))
    }

    "fail withValues when extra fields duplicate existing names" in {
      val baseSchema = Fields("a")
      val baseRow = Row.fromArray(baseSchema, Array(1))

      val extraFields = Fields("a") // duplicate field name
      val extraValues: Array[Any] = Array(2)

      an[IllegalArgumentException] shouldBe thrownBy(baseRow.withValues(extraFields, extraValues))
    }

    "append a single extra field via withValue" in {
      val baseSchema = Fields("a")
      val baseRow = Row.fromArray(baseSchema, Array(1))

      val extended = baseRow.withValue(Field("b"), 2)

      extended.schema.names.toSeq shouldBe Seq("a", "b")
      extended.values.toSeq shouldBe Seq(1, 2)

      extended.getAs[Int]("a") shouldBe Some(1)
      extended.getAs[Int]("b") shouldBe Some(2)
    }

    "fail withValue when the extra field already exists" in {
      val baseSchema = Fields("a")
      val baseRow = Row.fromArray(baseSchema, Array(1))

      an[IllegalArgumentException] shouldBe thrownBy(baseRow.withValue(Field("a"), 2)) // duplicate
    }

    "project and reorder columns by ordinal using selectAt (copy-based)" in {
      val schema = Fields("a", "b", "c")
      val row = Row.fromArray(schema, Array[Any](1, "x", true))

      val outSchema = Fields("c", "a")
      val ords = Array(2, 0)

      val projected = row.selectAt(outSchema, ords)
      projected.schema shouldBe outSchema
      projected.values.toSeq shouldBe Seq(true, 1)
      projected.get("c") shouldBe Some(true)
      projected.get("a") shouldBe Some(1)
    }

    "project and reorder columns by ordinal using viewAt without copying" in {
      val schema = Fields("a", "b", "c")
      val row = Row.fromArray(schema, Array[Any](1, "x", true))

      val outSchema = Fields("c", "a")
      val ords = Array(2, 0)

      val v = row.viewAt(outSchema, ords)
      v.schema shouldBe outSchema
      v.get("c") shouldBe Some(true)
      v.get("a") shouldBe Some(1)

      v.getAt(0) shouldBe true // ords(0)=2 -> base.getAt(2)
      v.getAt(1) shouldBe 1 // ords(1)=0 -> base.getAt(0)

      // prove it's a view: mutate base values and read through view
      row.values(2) = false
      v.get("c") shouldBe Some(false)
    }

    "support projection internals: -1 ordinals, null base values, and true view semantics" in {
      val schema = Fields("a", "b", "c")
      val base = Row.fromArray(schema, Array[Any](1, null, true))

      val ordsForCopy = Array(2, -1, 1) // c, missing, b(null)
      val outArr = new Array[Any](ordsForCopy.length)
      base.copyProjectedInto(ordsForCopy, outArr)
      outArr.toSeq shouldBe Seq(true, null, null)

      val outSchema = Fields("c", "missing", "b")
      val ordsForView = Array(2, -1, 1)
      val v = base.viewAt(outSchema, ordsForView)

      v.values.toSeq shouldBe Seq(true, null, null)
      v.values.length shouldBe 3 // view arity, not base arity

      v.get("does_not_exist") shouldBe None
      v.get("missing") shouldBe None
      v.get("b") shouldBe None
      v.getAt(1) shouldBe (null: Any)
      v.isNullAt(1) shouldBe true // j = -1
      v.isNullAt(2) shouldBe true // j = 1, base value is null
      v.isNullAt(0) shouldBe false // j = 2, base value true
    }

    "return correctly projected values from a view row (not the base array)" in {
      val schema = Fields("a", "b", "c")
      val base = Row.fromArray(schema, Array[Any](10, 20, 30))

      // project c, a (reverse two of the three columns, skip b)
      val outSchema = Fields("c", "a")
      val view = base.viewAt(outSchema, Array(2, 0))

      // values must align with the view's schema, not the base schema
      view.values.toSeq shouldBe Seq(30, 10)
      view.values.length shouldBe 2 // not 3 (base arity)

      // getAt must agree with values
      view.getAt(0) shouldBe 30
      view.getAt(1) shouldBe 10
    }

    "support withValues on a view row without corrupting field order" in {
      val schema = Fields("a", "b", "c")
      val base = Row.fromArray(schema, Array[Any](1, 2, 3))

      // view exposes only (c, a) in that order
      val view = base.viewAt(Fields("c", "a"), Array(2, 0))

      val extended = view.withValues(Fields("extra"), Array[Any](99))

      extended.schema.names.toSeq shouldBe Seq("c", "a", "extra")
      extended.values.toSeq shouldBe Seq(3, 1, 99)

      extended.getAs[Int]("c") shouldBe Some(3)
      extended.getAs[Int]("a") shouldBe Some(1)
      extended.getAs[Int]("extra") shouldBe Some(99)
    }

    "return independent values snapshots from a view row on each call" in {
      val schema = Fields("a", "b")
      val arr = Array[Any](10, 20)
      val base = Row.fromArray(schema, arr)
      val view = base.viewAt(Fields("b", "a"), Array(1, 0))

      val snap1 = view.values
      arr(0) = 99 // mutate base

      // snap1 is a copy taken before mutation — unaffected
      snap1.toSeq shouldBe Seq(20, 10)

      // fresh call reflects the mutation
      view.values.toSeq shouldBe Seq(20, 99)
    }

    "align to a target schema and fill missing columns with null when using alignTo view" in {
      val schema = Fields("a", "b")
      val row = Row.fromArray(schema, Array[Any](1, "x"))

      val target = Fields("b", "missing", "a")
      val alignedView = row.alignTo(target, copy = false)

      alignedView.schema shouldBe target
      alignedView.get("b") shouldBe Some("x")
      alignedView.get("a") shouldBe Some(1)
      alignedView.get("missing") shouldBe None
      alignedView.getAt(1) shouldBe (null: Any)
    }

    "align to a target schema using a copy-backed row when alignTo(copy=true)" in {
      val schema = Fields("a", "b")
      val row = Row.fromArray(schema, Array[Any](1, "x"))

      val target = Fields("b", "a")
      val aligned = row.alignTo(target, copy = true)

      aligned.values.toSeq shouldBe Seq("x", 1)

      // mutate base; copy should not change
      row.values(1) = "CHANGED"
      aligned.get("b") shouldBe Some("x")
    }
  }
}
