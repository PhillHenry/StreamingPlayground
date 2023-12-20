package uk.co.odinconsultants.kafka
import cats.effect.{Deferred, IO}
import com.github.dockerjava.api.DockerClient
import uk.co.odinconsultants.dreadnought.docker.KafkaRaft
import uk.co.odinconsultants.dreadnought.docker.Algebra.toInterpret
import uk.co.odinconsultants.dreadnought.docker.CatsDocker.{createNetwork, interpret, removeNetwork}
import uk.co.odinconsultants.dreadnought.docker.KafkaAntics.createCustomTopic
import uk.co.odinconsultants.dreadnought.docker.ContainerId
import uk.co.odinconsultants.dreadnought.docker.Logging.{LoggingLatch, ioPrintln, verboseWaitFor}
import uk.co.odinconsultants.sss.SSSUtils.{TIME_FORMATE, TOPIC_NAME}
import uk.co.odinconsultants.sss.SparkStructuredStreamingMain
import uk.co.odinconsultants.sss.SparkStructuredStreamingMain.OUTSIDE_KAFKA_BOOTSTRAP_PORT
import fs2.Stream
import fs2.kafka.*
import cats.effect.kernel.Ref
import cats.effect.std.CountDownLatch
import uk.co.odinconsultants.LoggingUtils.ioLog

import java.util.{Date, TimeZone}
import java.text.SimpleDateFormat
import scala.concurrent.duration.*

object KafkaUtils {

  type Loggers       = List[String => IO[Unit]]
  type LoggerFactory = Deferred[IO, String] => Loggers

  val bootstrapServer = s"localhost:${OUTSIDE_KAFKA_BOOTSTRAP_PORT}"

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
    _      <- kafkaStart.get.timeout(60.seconds)
    _      <- ioLog(s"About to create topic $TOPIC_NAME")
    _      <- IO(createCustomTopic(TOPIC_NAME, OUTSIDE_KAFKA_BOOTSTRAP_PORT, partitions = partitions))
  } yield kafkas

  def sendMessages(counter: Ref[IO, Int]): IO[Unit] = {
    val producerSettings: ProducerSettings[IO, String, String] =
      ProducerSettings[IO, String, String]
        .withBootstrapServers(bootstrapServer)
    TransactionalKafkaProducer
      .stream(
        TransactionalProducerSettings(
          s"transactionId${System.currentTimeMillis()}",
          producerSettings.withRetries(10),
        )
      )
      .flatMap { producer =>
        val messages: Stream[IO, ProducerResult[String, String]] =
          produceWithoutOffsets(producer, TOPIC_NAME, counter)
        messages
      }
      .handleErrorWith(x =>
        Stream.eval(ioLog("Failed to send messages") *> IO(x.printStackTrace()))
      )
      .compile
      .drain
  }

  def sendMessages(numMessages: Int): IO[Unit] = {
    val producerSettings: ProducerSettings[IO, String, String] =
      ProducerSettings[IO, String, String]
        .withBootstrapServers(bootstrapServer)
    transactionalProducerStream(producerSettings, TOPIC_NAME, numMessages)
      .handleErrorWith((x: Throwable) => Stream.eval(IO(x.printStackTrace())))
      .compile
      .drain
  }

  def transactionalProducerStream(
      producerSettings: ProducerSettings[IO, String, String],
      topic:            String,
      numMessages:      Int,
  ): Stream[IO, ProducerResult[String, String]] =
    TransactionalKafkaProducer
      .stream(
        TransactionalProducerSettings(
          s"transactionId${System.currentTimeMillis()}",
          producerSettings.withRetries(1),
        )
      )
      .flatMap { producer =>
        sendEachMessageInItsOwnTX(producer, topic, numMessages)
      }

  def produceWithoutOffsets(
      producer: TransactionalKafkaProducer.WithoutOffsets[IO, String, String],
      topic:    String,
      counter:  Ref[IO, Int],
  ): Stream[IO, ProducerResult[String, String]] = {
    val tz = TimeZone.getTimeZone("UTC")
    val df = new SimpleDateFormat(TIME_FORMATE)
    df.setTimeZone(tz)
    Stream.eval(counter.getAndUpdate(_ + 1)).evalMap { case i: Int =>
      ioLog(s"Sending message $i") *>
        producer.produceWithoutOffsets(
          ProducerRecords.one(ProducerRecord(topic, s"$i", df.format(new Date())))
        )
    }
  }

  private def sendEachMessageInItsOwnTX(
      producer:    TransactionalKafkaProducer[IO, String, String],
      topic:       String,
      numMessages: Int,
  ): Stream[IO, ProducerResult[String, String]] =
    eachMessageInItsOwnTX(topic, numMessages)
      .evalMap { case record =>
        ioLog(s"About to produce $record") *> producer.produce(record)
      }

  def eachMessageInItsOwnTX(
      topic:       String,
      numMessages: Int,
  ): Stream[IO, TransactionalProducerRecords[IO, String, String]] =
    Stream
      .emits((0 to numMessages - 1).zipWithIndex)
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
