package uk.co.odinconsultants.sss
import cats.effect.kernel.Ref
import cats.effect.std.CountDownLatch
import cats.effect.unsafe.implicits.global
import cats.effect.{Deferred, IO}
import fs2.kafka.{AutoOffsetReset, ConsumerSettings}
import org.scalatest.GivenWhenThen
import org.scalatest.wordspec.AnyWordSpec
import uk.co.odinconsultants.documentation_utils.SpecPretifier
import uk.co.odinconsultants.dreadnought.Flow.race
import uk.co.odinconsultants.dreadnought.docker.Algebra.toInterpret
import uk.co.odinconsultants.dreadnought.docker.CatsDocker.{createNetwork, removeNetwork}
import uk.co.odinconsultants.dreadnought.docker.{CatsDocker, StopRequest}
import uk.co.odinconsultants.kafka.KafkaUtils.{
  LoggerFactory,
  Loggers,
  bootstrapServer,
  consume,
  sendMessages,
  startKafkasAndWait,
}
import uk.co.odinconsultants.sss.SSSUtils.{MAX_EXECUTORS, TOPIC_NAME}
import uk.co.odinconsultants.sss.SparkStructuredStreamingMain.networkName
import uk.co.odinconsultants.LoggingUtils.ioLog

import scala.concurrent.duration.*

class KafkaSmokeSpec extends SpecPretifier with GivenWhenThen {

  val NUM_BROKERS    = 3
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

  val loggerLatch: LoggerFactory = { case kafkaStart: Deferred[IO, String] =>
    (1 until NUM_BROKERS)
      .map(i =>
        verboseWaitFor(s"kafka${i}: ")(
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
        _         <- IO(Given(s"a Kafka cluster with $NUM_BROKERS brokers and $NUM_PARTITIONS partitions running the RAFT protocol"))
        client    <- CatsDocker.client
        _         <- removeNetwork(client, networkName).handleErrorWith(x =>
                       ioLog(s"Did not delete network $networkName.\n${x.getMessage}")
                     )
        _         <- createNetwork(client, networkName)
        kafkas    <- startKafkasAndWait(client, networkName, NUM_PARTITIONS, loggerLatch(latch), latch)
        leader    <- latch.get
        leaderName = leader.substring(0, leader.indexOf(":"))
        _         <- IO(When(s"node '$leaderName' is elected leader"))

        numMessages = 100
        _          <- IO(And(s"and we send $numMessages messages")) *> sendMessages(numMessages)
        latch      <- CountDownLatch[IO](numMessages)
        _          <- IO(Then(s"$numMessages messages are received")) *> consume(
                        ConsumerSettings[IO, String, String]
                          .withAutoOffsetReset(AutoOffsetReset.Earliest)
                          .withBootstrapServers(bootstrapServer)
                          .withGroupId("group_PH"),
                        TOPIC_NAME,
                        latch,
                      ).compile.drain.start
        _          <- latch.await.timeout(2.minutes)

        _ <- race(toInterpret(client))(
               kafkas.map(StopRequest.apply)
             )
      } yield println(s"PH: Leader line: ${leader}")
      io.unsafeRunSync()
    }
  }

}
