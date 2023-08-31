package uk.co.odinconsultants.kafka

import cats.effect.{Deferred, IO, IOApp}
import cats.free.Free
import com.comcast.ip4s.{port, *}
import fs2.Stream
import fs2.kafka.ProducerSettings
import uk.co.odinconsultants.dreadnought.Flow.race
import uk.co.odinconsultants.dreadnought.docker.*
import uk.co.odinconsultants.dreadnought.docker.Algebra.toInterpret
import uk.co.odinconsultants.dreadnought.docker.CatsDocker.interpret
import uk.co.odinconsultants.dreadnought.docker.KafkaAntics.{createCustomTopic, produceMessages}
import uk.co.odinconsultants.dreadnought.docker.Logging.verboseWaitFor
import uk.co.odinconsultants.dreadnought.docker.PopularContainers.startKafkaOnPort
import uk.co.odinconsultants.dreadnought.docker.SparkStructuredStreamingMain.{startSlave, startSparkCluster, waitForMaster}
import uk.co.odinconsultants.dreadnought.docker.ZKKafkaMain.{kafkaEcosystem, startKafkaCluster}

import java.util.UUID
import scala.concurrent.duration.*

object KafkaDemoMain extends IOApp.Simple {

  val TOPIC_NAME = "test_topic"
  val BOOTSTRAP  = "kafka_bootstrap"
  val BROKER     = "zk"
  val kafkaPort  = port"9093"

  /** TODO
    * Pull images
    */
  def run: IO[Unit] = for {
    client       <- CatsDocker.client
    (zk, kafka1) <- startKafkaCluster(client, verboseWaitFor(Some(s"${Console.RED}SparkMaster: ")), 20.seconds)
    zkName       <- CatsDocker.interpret(client, Free.liftF(NamesRequest(zk)))
    kafkaStart   <- Deferred[IO, String]
    kafkaLatch    = verboseWaitFor(Some(s"${Console.BLUE}Kafka: "))("started (kafka.server.Kafka", kafkaStart)
    kafka2       <- interpret(client, Free.liftF(startKafkaOnPort(kafkaPort, zkName)))
    _            <- interpret(
                      client,
                      Free.liftF(
                        LoggingRequest(kafka2, kafkaLatch)
                      ),
                    )
    _            <- kafkaStart.get.timeout(20.seconds)
    _            <- sendMessages
    _            <- race(toInterpret(client))(
                      List(kafka2, kafka1, zk).map(StopRequest.apply)
                    )
  } yield println("Started and stopped ZK and 2 kafka brokers")

  private val sendMessages: IO[Unit] = {
    val bootstrapServer                                        = s"localhost:${kafkaPort.value}"
    val producerSettings: ProducerSettings[IO, String, String] =
      ProducerSettings[IO, String, String]
        .withBootstrapServers(bootstrapServer)
    val messages                                               = KafkaAntics
      .produce(producerSettings, TOPIC_NAME)
      .handleErrorWith(x => Stream.eval(IO(x.printStackTrace())))
      .compile
      .drain
    IO(createCustomTopic(TOPIC_NAME, port"9091")) *> messages
  }

}
