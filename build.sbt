ThisBuild / organization := "io.github.andy327"
ThisBuild / organizationName := "Andres Perez"
ThisBuild / organizationHomepage := Some(url("https://github.com/andy327/sparkling"))
ThisBuild / version := "0.2.0-SNAPSHOT"
ThisBuild / versionScheme := Some("early-semver")
ThisBuild / scalaVersion := "2.13.8"

ThisBuild / scalacOptions ++= {
  CrossVersion.partialVersion(scalaVersion.value) match {
    case Some((2, 12)) => Seq("-deprecation", "-Ywarn-unused", "-Ywarn-unused-import", "-language:higherKinds")
    case _             => Seq("-deprecation", "-Wunused", "-Wunused:imports")
  }
}

// SemanticDB enables Scalafix semantic rules
ThisBuild / semanticdbEnabled := true
ThisBuild / scalafixScalaBinaryVersion := CrossVersion.binaryScalaVersion(scalaVersion.value)

// Publishing
ThisBuild / homepage := Some(url("https://github.com/andy327/sparkling"))
ThisBuild / description := "Sparkling — a typed Scala DSL for Spark DataFrames"
ThisBuild / licenses := List("MIT" -> url("https://opensource.org/licenses/MIT"))
ThisBuild / developers := List(
  Developer("andy327", "Andres Perez", "andy327@gmail.com", url("https://github.com/andy327"))
)
ThisBuild / scmInfo := Some(
  ScmInfo(
    url("https://github.com/andy327/sparkling"),
    "scm:git@github.com:andy327/sparkling.git"
  )
)
ThisBuild / sonatypeCredentialHost := "central.sonatype.com"

addCommandAlias("formatAll", ";scalafixAll;scalafixAll;scalafmtAll;scalafmtAll;scalafmtSbt")
addCommandAlias("ci", ";+clean;scalafixAll --check;scalafmtCheckAll;scalafmtSbtCheck;coverage;+test;coverageReport")

val sparkVersion = "3.5.2"
val scalatestVersion = "3.2.19"
val algebirdVersion = "0.13.10"
val shapelessVersion = "2.3.10"
val zeroAllocHashingVersion = "0.16"

// JDK 17 module flags Spark needs
lazy val sparkJdk17Flags = Seq(
  "--add-exports=java.base/sun.nio.ch=ALL-UNNAMED",
  "--add-exports=java.base/jdk.internal.ref=ALL-UNNAMED",
  "--add-exports=java.base/sun.security.action=ALL-UNNAMED",
  "--add-opens=java.base/java.nio=ALL-UNNAMED",
  "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
  "--add-opens=java.base/java.lang=ALL-UNNAMED",
  "--add-opens=java.base/sun.security.action=ALL-UNNAMED"
)

lazy val noUnusedInConsoles = {
  def dropUnused(opts: Seq[String]) =
    opts.filterNot(o => o.startsWith("-Wunused") || o.startsWith("-Ywarn-unused"))
  Seq(
    Compile / console / scalacOptions := dropUnused((Compile / console / scalacOptions).value),
    Test / console / scalacOptions := dropUnused((Test / console / scalacOptions).value)
  )
}

lazy val sparkling = (project in file("."))
  .enablePlugins(spray.boilerplate.BoilerplatePlugin)
  .settings(
    name := "sparkling",
    crossScalaVersions := Seq("2.12.18", "2.13.8"),
    // Scaladoc: suppress unresolvable Java class links in 2.12's old Scaladoc tool
    Compile / doc / scalacOptions ++= {
      CrossVersion.partialVersion(scalaVersion.value) match {
        case Some((2, 12)) => Seq("-no-link-warnings")
        case _             => Nil
      }
    },
    libraryDependencies ++= Seq(
      "com.chuusai" %% "shapeless" % shapelessVersion,
      "net.openhft" % "zero-allocation-hashing" % zeroAllocHashingVersion,
      "com.twitter" %% "algebird-core" % algebirdVersion,
      "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
      "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
      "org.apache.spark" %% "spark-mllib" % sparkVersion % "provided",
      "org.scalatest" %% "scalatest" % scalatestVersion % Test
    ),
    publishTo := {
      if (isSnapshot.value)
        Some("Central Portal Snapshots".at("https://central.sonatype.com/repository/maven-snapshots/"))
      else
        sonatypePublishToBundle.value
    },
    useGpgPinentry := true,
    Test / fork := true,
    Test / envVars += ("SPARK_LOCAL_IP" -> "127.0.0.1"),
    Test / javaOptions ++= sparkJdk17Flags,
    Test / classLoaderLayeringStrategy := ClassLoaderLayeringStrategy.Flat
  )
  .settings(noUnusedInConsoles)
