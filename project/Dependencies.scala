import sbt.{Def, _}
import org.portablescala.sbtplatformdeps.PlatformDepsPlugin.autoImport._

object Dependencies {

  object V {
    val cats          = "2.8.0"
    val catsEffect    = "3.3.12"
    val circe         = "0.14.2"
    val ciris         = "2.3.2"
    val doobie        = "1.0.0-RC2"
    val flyway        = "8.5.11"
    val fs2Core       = "3.2.7"
    val fs2Kafka      = "2.4.0"
    val http4s        = "1.0.0-M32"
    val http4sWs      = "1.0.0-M1"
    val kittens       = "3.0.0-M1"
    val monocle       = "3.1.0"
    val natchez       = "0.1.6"
    val natchezHttp4s = "0.3.2"
    val neutron       = "0.6.0"
    val odin          = "0.13.0"
    val redis4cats    = "1.2.0"
    val refined       = "0.9.29"

    val scalajsTime = "2.4.0-M1"
    val tyrian      = "0.3.2"

    val scalacheck = "1.16.0"
    val weaver     = "0.7.12"

    val organizeImports = "0.6.0"

    val dockerJava  = "3.2.13"
    val ip4s        = "3.1.3"
    val dreadnought = "0.1.9"

    val spark     = "3.5.0"
    val hadoopAws = "3.3.1"
    val minio     = "8.5.5"
  }

  object Libraries {
    def circe(artifact: String): Def.Initialize[sbt.ModuleID] =
      Def.setting("io.circe" %%% ("circe-" + artifact) % V.circe)

    def http4s(artifact: String): ModuleID = "org.http4s" %% ("http4s-" + artifact) % V.http4s

    val cats       = "org.typelevel" %% "cats-core"           % V.cats
    val catsEffect = "org.typelevel" %% "cats-effect"         % V.catsEffect
    val fs2Core    = "co.fs2"        %% "fs2-core"            % V.fs2Core
    val kittens    = "org.typelevel" %% "kittens"             % V.kittens
    val testkit    = "org.typelevel" %% "cats-effect-testkit" % V.catsEffect % Test

    val cirisCore    = "is.cir" %% "ciris"         % V.ciris
    val cirisRefined = "is.cir" %% "ciris-refined" % V.ciris

    val circeCore    = circe("core")
    val circeParser  = circe("parser")
    val circeRefined = circe("refined")

    val doobieH2 = "org.tpolecat" %% "doobie-h2"   % V.doobie
    val flyway   = "org.flywaydb"  % "flyway-core" % V.flyway

    val http4sDsl     = http4s("dsl")
    val http4sServer  = http4s("ember-server")
    val http4sClient  = http4s("ember-client")
    val http4sCirce   = http4s("circe")
    val http4sMetrics = http4s("prometheus-metrics")

    val http4sJdkWs = "org.http4s" %% "http4s-jdk-http-client" % V.http4sWs

    val logBack = "ch.qos.logback" % "logback-classic" % "1.2.11"

    val natchezCore      = "org.tpolecat" %% "natchez-core"      % V.natchez
    val natchezHoneycomb = "org.tpolecat" %% "natchez-honeycomb" % V.natchez
    val natchezHttp4s    = "org.tpolecat" %% "natchez-http4s"    % V.natchezHttp4s

    val neutronCore       = "dev.profunktor" %% "neutron-core"       % V.neutron
    val redis4catsEffects = "dev.profunktor" %% "redis4cats-effects" % V.redis4cats

    val monocleCore = Def.setting("dev.optics" %%% "monocle-core" % V.monocle)

    val odin = "com.github.valskalla" %% "odin-core" % V.odin

    val refinedCore = Def.setting("eu.timepit" %%% "refined" % V.refined)
    val refinedCats = Def.setting("eu.timepit" %%% "refined-cats" % V.refined)

    // webapp
    val scalajsTime = Def.setting("io.github.cquiroz" %%% "scala-java-time" % V.scalajsTime)
    val tyrian      = Def.setting("io.indigoengine" %%% "tyrian" % V.tyrian)

    // test
    val monocleLaw       = "dev.optics"          %% "monocle-law"       % V.monocle
    val scalacheck       = "org.scalacheck"      %% "scalacheck"        % V.scalacheck
    val weaverCats       = "com.disneystreaming" %% "weaver-cats"       % V.weaver
    val weaverDiscipline = "com.disneystreaming" %% "weaver-discipline" % V.weaver
    val weaverScalaCheck = "com.disneystreaming" %% "weaver-scalacheck" % V.weaver

    // only for demo
    val fs2Kafka = "com.github.fd4s" %% "fs2-kafka" % V.fs2Kafka

    // scalafix rules
    val organizeImports = "com.github.liancheng" %% "organize-imports" % V.organizeImports

    val dockerJava          = "com.github.docker-java" % "docker-java-core" % V.dockerJava
    val dockerJavaTransport =
      "com.github.docker-java" % "docker-java-transport-httpclient5" % V.dockerJava

    val ip4s = "com.comcast" %% "ip4s-core" % V.ip4s

    def dreadnought(artifact: String): ModuleID =
      "uk.co.odinconsultants" %% ("dreadnought-" + artifact) % V.dreadnought
    val dreadnoughtCore     = dreadnought("core")
    val dreadnoughtDocker   = dreadnought("docker")
    val dreadnoughtExamples = dreadnought("examples")

    val spark      = ("org.apache.spark" %% "spark-sql" % V.spark).cross(CrossVersion.for3Use2_13)
    val sparkKafka =
      ("org.apache.spark" %% "spark-sql-kafka-0-10" % V.spark).cross(CrossVersion.for3Use2_13)
    val hadoopAws = ("org.apache.hadoop" % "hadoop-aws" % V.hadoopAws)
      .exclude("com.fasterxml.jackson.core", "jackson-databind")
    val minio     =
      ("io.minio" % "minio" % V.minio).exclude("com.fasterxml.jackson.core", "jackson-databind")
    val burningWave = "org.burningwave" % "tools" % "0.25.8"

  }

}
