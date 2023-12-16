import Dependencies._

ThisBuild / scalaVersion     := "3.1.1"
ThisBuild / version          := "0.1.0"
ThisBuild / organization     := "uk.co.odinconsultants"
ThisBuild / organizationName := "OdinConsultants"

ThisBuild / evictionErrorLevel := Level.Warn
ThisBuild / scalafixDependencies += Libraries.organizeImports

ThisBuild / semanticdbVersion := scalafixSemanticdb.revision

ThisBuild / resolvers += Resolver.sonatypeRepo("snapshots")

Compile / run / fork           := true

Global / onChangedBuildSource := ReloadOnSourceChanges
Global / semanticdbEnabled    := true // for metals

val dreadnoughtDependencies = Seq(
  Libraries.dreadnoughtCore,
  Libraries.dreadnoughtDocker,
  Libraries.dreadnoughtExamples
)

val sparkAndKafka = Seq(
  Libraries.spark,
  Libraries.sparkKafka,
  Libraries.hadoopAws,
)

val commonDependencies = Seq(
  Libraries.cats,
  Libraries.testkit,
  Libraries.catsEffect,
  Libraries.fs2Core,
  Libraries.fs2Kafka,
  Libraries.ip4s,
  Libraries.logBack,
  Libraries.minio,
  Libraries.burningWave,
  Libraries.documentationUtilsScalaTest,
  Libraries.scalaTest,
) ++ sparkAndKafka

val commonSettings = List(
  scalacOptions ++= List("-source:future"),
  scalafmtOnCompile := false, // recommended in Scala 3
  testFrameworks += new TestFramework("weaver.framework.CatsEffect"),
)

dependencyOverrides += "com.fasterxml.jackson.core" % "jackson-databind" % "2.12.3"
dependencyOverrides += "com.fasterxml.jackson.core" % "jackson-core"     % "2.12.3"

lazy val scala2V = "2.13.8"

lazy val root = (project in file("."))
  .settings(
    name := "StreamingPlayground"
  )
  .aggregate(lib, core, it)

lazy val lib = (project in file("modules/lib"))
  .settings((commonSettings ++ List(libraryDependencies := dreadnoughtDependencies ++ commonDependencies)): _*)

lazy val core = (project in file("modules/core"))
  .settings(commonSettings ++ List(libraryDependencies := dreadnoughtDependencies ++ commonDependencies): _*)
  .dependsOn(lib, scala2)

lazy val scala2 = (project in file("modules/scala2"))
  .settings(List(scalaVersion := scala2V) ++ List(libraryDependencies := sparkAndKafka :+ Libraries.burningWave): _*)

// integration tests
lazy val it = (project in file("modules/it"))
  .dependsOn(core)
  .settings(
    parallelExecution := false,
    commonSettings ++ List(
      libraryDependencies := List(
        "ch.qos.logback" % "logback-classic" % "1.2.11" % Test
      ) ++ dreadnoughtDependencies ++ commonDependencies,
      testOptions         := Seq(
        Tests.Argument(TestFrameworks.ScalaTest, "-fW", "mdocs/scenarios.txt")
      ),
    )
  )

val bddDocs = taskKey[Unit]("Turn the BDD output into HTML")

val header = s"""## Streaming Playground
                |
                |These are BDD (Behaviour Driven Design) tests that both test
                |the code and generate human readable documentation.
                |The code for these tests can be found in [GitHub](https://github.com/PhillHenry/StreamingPlayground/)
                |
                |""".stripMargin
val args   =
  " uk.co.odinconsultants.documentation_utils.SplitScenariosMain \"" + header + "\" mdocs/scenarios.txt"

bddDocs := Def.taskDyn {
  val appName = name.value
  Def.task {
    (runMain in core in Compile)
      .toTask(args)
      .value
  }
}.value

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
