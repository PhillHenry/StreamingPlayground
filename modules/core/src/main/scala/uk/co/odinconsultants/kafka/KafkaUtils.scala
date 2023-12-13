package uk.co.odinconsultants.kafka
import cats.effect.{Deferred, IO}
import com.github.dockerjava.api.DockerClient
import uk.co.odinconsultants.dreadnought.docker.KafkaRaft
import uk.co.odinconsultants.dreadnought.docker.Algebra.toInterpret
import uk.co.odinconsultants.dreadnought.docker.CatsDocker.{createNetwork, interpret, removeNetwork}
import uk.co.odinconsultants.dreadnought.docker.KafkaAntics.createCustomTopic
import uk.co.odinconsultants.dreadnought.docker.ContainerId
import uk.co.odinconsultants.dreadnought.docker.Logging.{LoggingLatch, ioPrintln, verboseWaitFor}
import uk.co.odinconsultants.sss.SSSUtils.TOPIC_NAME
import uk.co.odinconsultants.sss.SparkStructuredStreamingMain
import uk.co.odinconsultants.sss.SparkStructuredStreamingMain.OUTSIDE_KAFKA_BOOTSTRAP_PORT

import scala.concurrent.duration.*

object KafkaUtils {

  type Loggers = List[String => IO[Unit]]
  type LoggerFactory = Deferred[IO, String] => Loggers

  def createLoggers(kafkaStart: Deferred[IO, String]): List[
    String => IO[Unit]
  ] = {
    val kafkaLatch =
      verboseWaitFor(Some(s"${Console.BLUE}kafka1: "))("started (kafka.server.Kafka", kafkaStart)
    List(
      kafkaLatch,
      ioPrintln(Some(s"${Console.GREEN}kafka2: ")),
      ioPrintln(Some(s"${Console.YELLOW}kafka3: ")),
    )
  }

  def startKafkas(
      client:      DockerClient,
      networkName: String,
      partitions:  Int = 2,
  ): IO[List[ContainerId]] = for {
    kafkaStart <- Deferred[IO, String]
    loggers     = createLoggers(kafkaStart)
    ids        <- startKafkasAndWait(client, networkName, partitions, loggers, kafkaStart)
  } yield ids

  def startKafkasAndWait(
      client:      DockerClient,
      networkName: String,
      partitions:  Int,
      loggers:     Loggers,
      kafkaStart:  Deferred[IO, String],
  ): IO[List[ContainerId]] = for {
    kafkas <-
      interpret(
        client,
        KafkaRaft.startKafkas(loggers, networkName),
      )
    _      <- kafkaStart.get.timeout(20.seconds)
    _      <- SparkStructuredStreamingMain.ioLog(s"About to create topic $TOPIC_NAME")
    _      <- IO(createCustomTopic(TOPIC_NAME, OUTSIDE_KAFKA_BOOTSTRAP_PORT, partitions = partitions))
  } yield kafkas

}
