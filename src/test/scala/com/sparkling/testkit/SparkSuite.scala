package com.sparkling.testkit

import java.io.File
import java.nio.file.{Files, Path, Paths}

import org.apache.spark.sql.types._
import org.apache.spark.sql.{functions => sqlf, Column, DataFrame, SparkSession}
import org.scalatest.{BeforeAndAfterAll, Suite}

trait SparkSuite extends BeforeAndAfterAll { this: Suite =>

  implicit protected lazy val spark: SparkSession =
    SparkSession.builder()
      .appName(this.getClass.getSimpleName.stripSuffix("$"))
      .master("local[1]")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.debug.maxToStringFields", "2000")
      .config("spark.sql.shuffle.partitions", "1")
      .getOrCreate()

  /** Resolve a classpath resource to an absolute file path. */
  final protected def resourcePath(path: String): String = {
    val url = this.getClass.getResource(path)
    require(url != null, s"Missing test resource: $path")
    new File(url.toURI).getAbsolutePath
  }

  /** Create a temp directory under repo-local `tmp/` (so you can find it easily). */
  final protected def repoTmpDir(prefix: String): String = {
    val base: Path = Paths.get("tmp")
    Files.createDirectories(base)
    Files.createTempDirectory(base, prefix).toFile.getAbsolutePath
  }

  /** Write a DataFrame as JSON to a temp dir under `tmp/` and read it back. */
  final protected def roundTripJsonToTmp(df: DataFrame): DataFrame = {
    val base = Paths.get("tmp")
    Files.createDirectories(base)

    val dir = Files.createTempDirectory(base, "spark_test_json_")
    val outPath = dir.toAbsolutePath.toString

    df.coalesce(1)
      .write
      .mode("overwrite")
      .json(outPath)

    spark.read.json(outPath)
  }

  /** Asserts two DataFrames contain the same rows (column order ignored).
    *
    * Canonicalizes MAP/ARRAY/STRUCT to JSON strings so equality and ordering are stable.
    */
  final protected def assertSameRows(a: DataFrame, b: DataFrame): Unit = {
    val aCols = a.columns.toSet
    val bCols = b.columns.toSet

    require(
      aCols == bCols,
      s"Schema mismatch.\nLeft:  ${a.columns.toSeq}\nRight: ${b.columns.toSeq}"
    )

    val orderedCols = aCols.toSeq.sorted

    def canonicalCol(df: DataFrame, name: String): Column = {
      val dt = df.schema(name).dataType
      val c = sqlf.col(name)

      dt match {
        case _: MapType =>
          sqlf.to_json(sqlf.sort_array(sqlf.map_entries(c)))
        case _: ArrayType =>
          sqlf.to_json(c)
        case _: StructType =>
          sqlf.to_json(c)
        case _ =>
          c.cast("string")
      }
    }

    def canonicalize(df: DataFrame): DataFrame =
      df.select(orderedCols.map(c => canonicalCol(df, c).as(c)): _*)

    def withDeterministicSort(df: DataFrame): DataFrame = {
      val keyParts = orderedCols.map(c => sqlf.col(c))
      df.withColumn("__row_sort_key__", sqlf.concat_ws("", keyParts: _*))
        .orderBy(sqlf.col("__row_sort_key__"))
        .drop("__row_sort_key__")
    }

    val left = withDeterministicSort(canonicalize(a)).collect().toSeq
    val right = withDeterministicSort(canonicalize(b)).collect().toSeq

    assert(left == right, s"Rows differ.\nLeft: $left\nRight: $right")
  }

  override protected def afterAll(): Unit = {
    try {
      spark.sharedState.cacheManager.clearCache()
      spark.stop()
    } finally
      super.afterAll()
  }
}
