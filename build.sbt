import Dependencies._

ThisBuild / scalaVersion     := "2.13.10"
ThisBuild / version          := "0.1.0-SNAPSHOT"
ThisBuild / organization     := "com.example"
ThisBuild / organizationName := "example"

lazy val root = (project in file("."))
  .settings(
    name := "streaming_iceberg",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-sql" % "3.3.2",
      "org.apache.iceberg" %% "iceberg-spark-runtime-3.3" % "1.2.1",
      "org.scala-lang.modules" %% "scala-collection-compat" % "2.10.0"
    )
  )
