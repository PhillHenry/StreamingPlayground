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
    client     <- CatsDocker.client
    kafkas     <- KafkaUtils.startKafkas(client, networkName)
    numMessages = 100
    _          <- sendMessages(numMessages)
    latch      <- CountDownLatch[IO](numMessages)
    _          <- consume(
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

  def sendMessages(numMessages: Int): IO[Unit] = {
    val producerSettings: ProducerSettings[IO, String, String] =
      ProducerSettings[IO, String, String]
        .withBootstrapServers(bootstrapServer)
    transactionalProducerStream(producerSettings, TOPIC_NAME, numMessages)
      .handleErrorWith((x: Throwable) => Stream.eval(IO(x.printStackTrace())))
      .compile
      .drain
  }

  private def transactionalProducerStream(
      producerSettings: ProducerSettings[IO, String, String],
      topic:            String,
      numMessages:      Int,
  ) =
    TransactionalKafkaProducer
      .stream(
        TransactionalProducerSettings(
          s"transactionId${System.currentTimeMillis()}",
          producerSettings.withRetries(10),
        )
      )
      .flatMap { producer =>
        sendEachMessageInItsOwnTX(producer, topic, numMessages)
      }

  private def sendEachMessageInItsOwnTX(
      producer:    TransactionalKafkaProducer[IO, String, String],
      topic:       String,
      numMessages: Int,
  ) =
    eachMessageInItsOwnTX(topic, numMessages)
      .evalMap { case record =>
        producer.produce(record)
      }

  def eachMessageInItsOwnTX(
      topic:       String,
      numMessages: Int,
  ): Stream[IO, TransactionalProducerRecords[IO, String, String]] =
    Stream
      .emits((1 to numMessages).zipWithIndex)
      .map { case (k, v) =>
        TransactionalProducerRecords.one(
          CommittableProducerRecords.one(
            ProducerRecord(topic, s"key_$k", s"val_$v"),
            CommittableOffset[IO](
              new org.apache.kafka.common.TopicPartition(topic, 1),
              new org.apache.kafka.clients.consumer.OffsetAndMetadata(1),
              Some("group"),
              x => IO.println(s"offset/partition = $x"),
            ),
          )
        )
      }
      .covary[IO]

  def consume(
      consumerSettings: ConsumerSettings[IO, String, String],
      topic:            String,
      latch:            CountDownLatch[IO],
  ): Stream[IO, CommittableConsumerRecord[IO, String, String]] =
    KafkaConsumer
      .stream(consumerSettings)
      .subscribeTo(topic)
      .records
      .evalMap { (committable: CommittableConsumerRecord[IO, String, String]) =>
        val record: ConsumerRecord[String, String] = committable.record
        ioLog(
          s"Consumed ${record.key} -> ${record.value}, offset = ${record.offset}, partition = ${record.partition}"
        ) *> latch.release *> IO(committable)
      }
}
