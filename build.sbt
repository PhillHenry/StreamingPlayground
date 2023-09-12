import Dependencies._

ThisBuild / scalaVersion     := "3.1.1"
ThisBuild / version          := "0.1.0"
ThisBuild / organization     := "uk.co.odinconsultants"
ThisBuild / organizationName := "OdinConsultants"

ThisBuild / evictionErrorLevel := Level.Warn
ThisBuild / scalafixDependencies += Libraries.organizeImports

ThisBuild / resolvers += Resolver.sonatypeRepo("snapshots")

Compile / run / fork           := true

Global / onChangedBuildSource := ReloadOnSourceChanges
Global / semanticdbEnabled    := true // for metals

val commonSettings = List(
  scalacOptions ++= List("-source:future"),
  scalafmtOnCompile := false, // recommended in Scala 3
  testFrameworks += new TestFramework("weaver.framework.CatsEffect"),
  libraryDependencies ++= Seq(
    Libraries.spark,
    Libraries.sparkKafka,
    Libraries.cats,
    Libraries.testkit,
    Libraries.catsEffect,
    Libraries.fs2Core,
    Libraries.fs2Kafka,
    Libraries.ip4s,
    Libraries.logBack,
    Libraries.dreadnoughtCore,
    Libraries.dreadnoughtDocker,
    Libraries.dreadnoughtExamples,
    Libraries.hadoopAws,
    Libraries.minio,
  )
)

dependencyOverrides += "com.fasterxml.jackson.core" % "jackson-databind" % "2.12.3"
dependencyOverrides += "com.fasterxml.jackson.core" % "jackson-core" % "2.12.3"

lazy val root = (project in file("."))
  .settings(
    name := "StreamingPlayground"
  )
  .aggregate(lib, core, it)

lazy val lib = (project in file("modules/lib"))
  .settings(commonSettings: _*)

lazy val core = (project in file("modules/core"))
  .settings(commonSettings: _*)
  .dependsOn(lib)

// integration tests
lazy val it = (project in file("modules/it"))
  .settings(commonSettings: _*)
  .dependsOn(core)
  .settings(
    libraryDependencies ++= List(
      "ch.qos.logback" % "logback-classic" % "1.2.11" % Test
    )
  )

lazy val docs = project
  .in(file("docs"))
  .settings(
    mdocIn        := file("modules/docs"),
    mdocOut       := file("target/docs"),
    mdocVariables := Map("VERSION" -> version.value),
  )
  .dependsOn(core)
  .enablePlugins(MdocPlugin)

addCommandAlias("runLinter", ";scalafixAll --rules OrganizeImports")
