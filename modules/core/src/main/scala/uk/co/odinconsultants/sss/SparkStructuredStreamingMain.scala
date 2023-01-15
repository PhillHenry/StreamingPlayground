package uk.co.odinconsultants.sss
import cats.effect.{Deferred, IO, IOApp}
import cats.free.Free
import com.comcast.ip4s.*
import fs2.kafka.{ConsumerSettings, ProducerRecords, ProducerSettings, *}
import fs2.{Chunk, Pipe, Pure, Stream}
import uk.co.odinconsultants.dreadnought.docker.ZKKafkaMain.waitFor
import uk.co.odinconsultants.dreadnought.docker.{CatsDocker, Command, ContainerId, ImageName, KafkaAntics, LoggingRequest, ManagerRequest, StartRequest, StopRequest, ZKKafkaMain}

object SparkStructuredStreamingMain extends IOApp.Simple {
  def run: IO[Unit] = for {
    client      <- CatsDocker.client
    sparkStart  <- Deferred[IO, String]
    spark       <- CatsDocker.interpret(client, startSpark(sparkStart))
    (zk, kafka) <- ZKKafkaMain.waitForStack(client)
    _           <- KafkaAntics
                     .produceMessages(ip"127.0.0.1", port"9092")
                     .handleErrorWith(x => Stream.eval(IO(x.printStackTrace())))
                     .compile
                     .drain
    _           <- CatsDocker.interpret(client, ZKKafkaMain.tearDownFree(zk, kafka))
    _           <- CatsDocker.interpret(client, Free.liftF(StopRequest(spark)))
  } yield println("Started and stopped")

  def startSpark(sparkStart: Deferred[IO, String]): Free[ManagerRequest, ContainerId] =
    for {
      spark <- Free.liftF(
          StartRequest(
            ImageName("bde2020/spark-master:3.2.1-hadoop3.2"),
            Command(""),
            List("INIT_DAEMON_STEP=setup_spark"),
            List.empty,
            List.empty
          )
        )
      _     <- Free.liftF(
                 LoggingRequest(spark, waitFor("started (kafka.server.KafkaServer)", sparkStart))
               )
    } yield spark
}
