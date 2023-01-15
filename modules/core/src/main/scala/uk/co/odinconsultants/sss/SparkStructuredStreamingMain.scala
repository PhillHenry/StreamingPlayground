package uk.co.odinconsultants.sss
import cats.effect.{IO, IOApp}
import com.comcast.ip4s.*
import fs2.kafka.{ConsumerSettings, ProducerRecords, ProducerSettings, *}
import fs2.{Chunk, Pipe, Pure, Stream}
import uk.co.odinconsultants.dreadnought.docker.{CatsDocker, ZKKafkaMain, KafkaAntics}

object SparkStructuredStreamingMain extends IOApp.Simple {
  def run: IO[Unit] = for {
    client      <- CatsDocker.client
    (zk, kafka) <- ZKKafkaMain.waitForStack(client)
    _           <- KafkaAntics.produceMessages(ip"127.0.0.1", port"9092")
                     .handleErrorWith(x => Stream.eval(IO(x.printStackTrace())))
                     .compile
                     .drain
    _           <- CatsDocker.interpret(client, ZKKafkaMain.tearDownFree(zk, kafka))
  } yield println("Started and stopped")
}
