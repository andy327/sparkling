package com.sparkling.syntax

import org.apache.spark.sql.{functions => sqlf}
import org.scalatest.matchers.should.Matchers._
import org.scalatest.wordspec.AnyWordSpec

import com.sparkling.testkit.SparkSuite

final class UserDefinedFunctionSyntaxSpec extends AnyWordSpec with SparkSuite {
  import UserDefinedFunctionSyntax._

  "UserDefinedFunctionSyntax" should {
    "leave the UDF deterministic when withDeterminism(true) is used" in {
      val u0 = sqlf.udf((i: Int) => i + 1)
      val u1 = u0.withDeterminism(true)

      u1.deterministic shouldBe u0.deterministic
    }

    "mark the UDF as non-deterministic when withDeterminism(false) is used" in {
      val u0 = sqlf.udf((i: Int) => i + 1)
      val u1 = u0.withDeterminism(false)

      u1.deterministic shouldBe false
    }

    "leave the UDF nullable flag unchanged when withNullable(true) is used" in {
      val u0 = sqlf.udf((i: Int) => i + 1)
      val u1 = u0.withNullable(true)

      u1.nullable shouldBe u0.nullable
    }

    "mark the UDF as non-nullable when withNullable(false) is used" in {
      val u0 = sqlf.udf((i: Int) => i + 1)
      val u1 = u0.withNullable(false)

      u1.nullable shouldBe false
    }

    "keep a nullable UDF nullable when withNullable(true) is used" in {
      val u0 = sqlf.udf((i: java.lang.Integer) => if (i == null) null else (i + 1): java.lang.Integer)

      u0.nullable shouldBe true

      val u1 = u0.withNullable(true)
      u1.nullable shouldBe true
    }

    "allow chaining of determinism and nullability modifiers" in {
      val u = sqlf.udf((i: java.lang.Integer) => if (i == null) null else (i + 1): java.lang.Integer)
        .withDeterminism(false)
        .withNullable(false)

      u.deterministic shouldBe false
      u.nullable shouldBe false
    }
  }
}
