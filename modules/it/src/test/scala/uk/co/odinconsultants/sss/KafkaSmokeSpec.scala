package uk.co.odinconsultants.sss
import cats.data.NonEmptyList
import cats.effect.kernel.Ref
import cats.effect.std.CountDownLatch
import cats.effect.unsafe.implicits.global
import cats.effect.{Deferred, IO}
import cats.free.Free
import fs2.kafka.{AutoOffsetReset, ConsumerSettings}
import org.scalatest.GivenWhenThen
import org.scalatest.wordspec.AnyWordSpec
import uk.co.odinconsultants.LoggingUtils.ioLog
import uk.co.odinconsultants.documentation_utils.SpecPretifier
import uk.co.odinconsultants.dreadnought.Flow.{process, race}
import uk.co.odinconsultants.dreadnought.docker.Algebra.toInterpret
import uk.co.odinconsultants.dreadnought.docker.CatsDocker.{createNetwork, removeNetwork}
import uk.co.odinconsultants.dreadnought.docker.{CatsDocker, StopRequest}
import uk.co.odinconsultants.kafka.KafkaUtils.*
import uk.co.odinconsultants.sss.SSSUtils.{MAX_EXECUTORS, TOPIC_NAME}
import uk.co.odinconsultants.sss.SparkStructuredStreamingMain.{OUTSIDE_KAFKA_BOOTSTRAP_PORT, networkName}

import scala.concurrent.duration.*

class KafkaSmokeSpec extends SpecPretifier with GivenWhenThen {

  val NUM_BROKERS    = 4
  val NUM_PARTITIONS = 2

  def verboseWaitFor(
      name: String
  )(seek:   String, deferred: Deferred[IO, String]): String => IO[Unit] = {
    val printer = IO.println
    (line: String) =>
      val withMachine = s"${name}$line"
      if (line.contains(seek)) printer(s"Started!\n$line") *> deferred.complete(withMachine).void
      else printer(withMachine)
  }

  val BROKER_NAMES = (1 to NUM_BROKERS).map(i => s"kafka${i}")

  val loggerLatch: LoggerFactory = { case kafkaStart: Deferred[IO, String] =>
    BROKER_NAMES
      .map(name =>
        verboseWaitFor(s"$name: ")(
          "] (org.apache.kafka.raft.LeaderState)",
          kafkaStart,
        )
      )
      .toList
  }

  "A kafka cluster" should {
    "broker messages" in {
      val io = for {
        latch     <- Deferred[IO, String]
        loggers    = loggerLatch(latch)
        _         <-
          IO(
            Given(
              s"a Kafka cluster with ${loggers.length} brokers and $NUM_PARTITIONS partitions running the RAFT protocol"
            )
          )
        client    <- CatsDocker.client
        _         <- removeNetwork(client, networkName).handleErrorWith(x =>
                       ioLog(s"Did not delete network $networkName.\n${x.getMessage}")
                     )
        _         <- createNetwork(client, networkName)
        kafkas    <- startKafkasAndWait(client, networkName, NUM_PARTITIONS, loggers, latch)
        leader    <- latch.get
        leaderName = leader.substring(0, leader.indexOf(":"))
        _         <-
          IO(
            When(
              s"node '$leaderName' is elected leader from the cluster of brokers, {${kafkas.mkString(", ")}"
            )
          )

        numMessages = 100
        _          <- IO(And(s"we send $numMessages messages")) *> sendMessages(numMessages)

        leaderIds = kafkas.zip(BROKER_NAMES).filter(_._2.startsWith(leaderName)).map(_._1)
        _        <- IO(And(s"we then kill the leader with ID '${leaderIds.mkString(",")}'"))
        _        <- toInterpret(client)(Free.liftF(StopRequest(leaderIds.head)))

        latch <- CountDownLatch[IO](numMessages)
        _     <- IO(Then(s"$numMessages messages are received")) *> consume(
                   ConsumerSettings[IO, String, String]
                     .withAutoOffsetReset(AutoOffsetReset.Earliest)
                     .withBootstrapServers((0 until NUM_PARTITIONS).map(i => s"localhost:${i + OUTSIDE_KAFKA_BOOTSTRAP_PORT.value}").mkString(","))
                     .withGroupId("group_PH"),
                   TOPIC_NAME,
                   latch,
                 ).compile.drain.start
        _     <- latch.await.timeout(2.minutes)

        _ <- race(toInterpret(client))(
               kafkas.filter(x => !leaderIds.contains(x)).map(StopRequest.apply)
             )
      } yield println(s"PH: Leader line: ${leader}")
      io.unsafeRunSync()
    }
  }

}
