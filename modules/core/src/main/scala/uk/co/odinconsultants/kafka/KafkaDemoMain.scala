package uk.co.odinconsultants.kafka

import cats.effect.{Deferred, IO, IOApp}
import cats.free.Free
import com.comcast.ip4s.{port, *}
import fs2.Stream
import fs2.kafka.{
  AutoOffsetReset,
  CommittableConsumerRecord,
  ConsumerRecord,
  ConsumerSettings,
  KafkaConsumer,
  ProducerSettings,
}
import uk.co.odinconsultants.dreadnought.Flow.race
import uk.co.odinconsultants.dreadnought.docker.*
import uk.co.odinconsultants.dreadnought.docker.Algebra.toInterpret
import uk.co.odinconsultants.dreadnought.docker.CatsDocker.interpret
import uk.co.odinconsultants.dreadnought.docker.KafkaAntics.{createCustomTopic, produceMessages}
import uk.co.odinconsultants.dreadnought.docker.Logging.{ioPrintln, verboseWaitFor}
import uk.co.odinconsultants.dreadnought.docker.PopularContainers.startKafkaOnPort
import uk.co.odinconsultants.dreadnought.docker.SparkStructuredStreamingMain.{
  startSlave,
  startSparkCluster,
  waitForMaster,
}
import uk.co.odinconsultants.dreadnought.docker.ZKKafkaMain.{kafkaEcosystem, startKafkaCluster}
import uk.co.odinconsultants.sss.SSSUtils.TOPIC_NAME
import uk.co.odinconsultants.sss.SparkStructuredStreamingMain.{
  OUTSIDE_KAFKA_BOOTSTRAP_PORT,
  ioLog,
  networkName,
}

import java.util.UUID
import scala.concurrent.duration.*

object KafkaDemoMain extends IOApp.Simple {

  val BROKER          = "zk"
  val kafkaPort       = port"9111"
  val bootstrapServer = s"localhost:${kafkaPort.value}"

  /** TODO
    * Pull images
    */
  def run: IO[Unit] = for {
    client <- CatsDocker.client
    kafkas <- KafkaUtils.startKafkas(client, networkName)
    _      <- sendMessages
    _      <- consume(
                ConsumerSettings[IO, String, String]
                  .withAutoOffsetReset(AutoOffsetReset.Earliest)
                  .withBootstrapServers(bootstrapServer)
                  .withGroupId("group_PH"),
                TOPIC_NAME,
              ).compile.drain.start
    _      <- ioLog("About to close down. Press return to end") *> IO.readLine
    _      <- race(toInterpret(client))(
                kafkas.map(StopRequest.apply)
              )
  } yield println("Started and stopped ZK and 2 kafka brokers")

  private val sendMessages: IO[Unit] = {
    val producerSettings: ProducerSettings[IO, String, String] =
      ProducerSettings[IO, String, String]
        .withBootstrapServers(bootstrapServer)
    val messages: IO[Unit]                                     = KafkaAntics
      .produce(producerSettings, TOPIC_NAME)
      .handleErrorWith((x: Throwable) => Stream.eval(IO(x.printStackTrace())))
      .compile
      .drain
    IO(createCustomTopic(TOPIC_NAME, kafkaPort)) *> messages
  }

  def consume(
      consumerSettings: ConsumerSettings[IO, String, String],
      topic:            String,
  ): Stream[IO, CommittableConsumerRecord[IO, String, String]] =
    KafkaConsumer
      .stream(consumerSettings)
      .subscribeTo(topic)
      .records
      .evalMap { (committable: CommittableConsumerRecord[IO, String, String]) =>
        val record: ConsumerRecord[String, String] = committable.record
        ioLog(s"Consumed ${record.key} -> ${record.value}") *> IO(committable)
      }
}
