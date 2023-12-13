package uk.co.odinconsultants.kafka

import cats.effect.std.CountDownLatch
import cats.effect.{Deferred, IO, IOApp}
import cats.free.Free
import com.comcast.ip4s.{port, *}
import fs2.Stream
import fs2.kafka.{
  AutoOffsetReset,
  CommittableConsumerRecord,
  CommittableOffset,
  CommittableProducerRecords,
  ConsumerRecord,
  ConsumerSettings,
  KafkaConsumer,
  ProducerRecord,
  ProducerResult,
  ProducerSettings,
  TransactionalKafkaProducer,
  TransactionalProducerRecords,
  TransactionalProducerSettings,
}
import uk.co.odinconsultants.dreadnought.Flow.race
import uk.co.odinconsultants.dreadnought.docker.{CatsDocker, StopRequest}
import uk.co.odinconsultants.dreadnought.docker.Algebra.toInterpret
import uk.co.odinconsultants.dreadnought.docker.CatsDocker.interpret
import uk.co.odinconsultants.dreadnought.docker.KafkaAntics.createCustomTopic
import uk.co.odinconsultants.dreadnought.docker.Logging.{ioPrintln, verboseWaitFor}
import uk.co.odinconsultants.dreadnought.docker.PopularContainers.startKafkaOnPort
import uk.co.odinconsultants.dreadnought.docker.SparkStructuredStreamingMain.{
  startSlave,
  startSparkCluster,
  waitForMaster,
}
import uk.co.odinconsultants.dreadnought.docker.ZKKafkaMain.{kafkaEcosystem, startKafkaCluster}
import uk.co.odinconsultants.sss.SSSUtils.TOPIC_NAME
import uk.co.odinconsultants.LoggingUtils.{ioLog, toMessage}
import uk.co.odinconsultants.kafka.KafkaUtils.{startKafkas, consume, sendMessages}
import uk.co.odinconsultants.sss.SparkStructuredStreamingMain.{
  OUTSIDE_KAFKA_BOOTSTRAP_PORT,
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
    client     <- CatsDocker.client
    kafkas     <- startKafkas(client, networkName)
    numMessages = 100
    _          <- ioLog("About to send messages...") *> sendMessages(numMessages)
    latch      <- CountDownLatch[IO](numMessages)
    _          <- ioLog("About to read messages...") *> consume(
                    ConsumerSettings[IO, String, String]
                      .withAutoOffsetReset(AutoOffsetReset.Earliest)
                      .withBootstrapServers(bootstrapServer)
                      .withGroupId("group_PH"),
                    TOPIC_NAME,
                    latch,
                  ).compile.drain.start
    _          <- latch.await
    _          <- race(toInterpret(client))(
                    kafkas.map(StopRequest.apply)
                  )
  } yield println("Started and stopped ZK and 2 kafka brokers")

}
