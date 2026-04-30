ThisBuild / organization := "com.sparkling"
ThisBuild / version := "0.1.0-SNAPSHOT"
ThisBuild / scalaVersion := "2.13.8"

// Required for Scalafix semantic rules
ThisBuild / semanticdbEnabled := true
ThisBuild / scalafixScalaBinaryVersion := CrossVersion.binaryScalaVersion((ThisBuild / scalaVersion).value)

ThisBuild / coverageEnabled := true

ThisBuild / scalacOptions ++= Seq(
  "-deprecation",
  "-Wunused",
  "-Wunused:imports"
)

addCommandAlias("formatAll", ";scalafixAll;scalafixAll;scalafmtAll;scalafmtAll;scalafmtSbt")
addCommandAlias("ci", ";clean;scalafixAll --check;scalafmtCheckAll;scalafmtSbtCheck;coverage;test;coverageAggregate")

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
  def dropUnused(opts: Seq[String]) = opts.filterNot(_.startsWith("-Wunused"))
  Seq(
    Compile / console / scalacOptions := dropUnused((Compile / console / scalacOptions).value),
    Test / console / scalacOptions := dropUnused((Test / console / scalacOptions).value)
  )
}

lazy val sparkling = (project in file("."))
  .enablePlugins(spray.boilerplate.BoilerplatePlugin)
  .settings(
    name := "sparkling",
    libraryDependencies ++= Seq(
      "com.chuusai" %% "shapeless" % shapelessVersion,
      "net.openhft" % "zero-allocation-hashing" % zeroAllocHashingVersion,
      "com.twitter" %% "algebird-core" % algebirdVersion,
      "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
      "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
      "org.apache.spark" %% "spark-mllib" % sparkVersion % "provided",
      "org.scalatest" %% "scalatest" % scalatestVersion % Test
    ),
    Test / fork := true,
    Test / envVars += ("SPARK_LOCAL_IP" -> "127.0.0.1"),
    Test / javaOptions ++= sparkJdk17Flags,
    Test / classLoaderLayeringStrategy := ClassLoaderLayeringStrategy.Flat
  )
  .settings(noUnusedInConsoles)
