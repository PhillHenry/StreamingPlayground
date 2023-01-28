package uk.co.odinconsultants.sss
import cats.effect.{Deferred, IO, IOApp}
import cats.free.Free
import com.comcast.ip4s.*
import fs2.kafka.{ConsumerSettings, ProducerRecords, ProducerSettings, *}
import fs2.{Chunk, Pipe, Pure, Stream}
import uk.co.odinconsultants.dreadnought.docker.ZKKafkaMain.startKafkaCluster
import uk.co.odinconsultants.dreadnought.docker.{
  CatsDocker,
  Command,
  ContainerId,
  ImageName,
  KafkaAntics,
  LoggingRequest,
  ManagerRequest,
  NamesRequest,
  StartRequest,
  StopRequest,
  ZKKafkaMain,
}
import com.comcast.ip4s.Port
import com.github.dockerjava.api.DockerClient
import scala.concurrent.duration.*
import cats.*
import cats.data.*
import cats.syntax.all.*
import uk.co.odinconsultants.dreadnought.Flow.race
import uk.co.odinconsultants.dreadnought.docker.Algebra.toInterpret
import uk.co.odinconsultants.dreadnought.docker.Logging.verboseWaitFor
import uk.co.odinconsultants.dreadnought.docker.SparkStructuredStreamingMain.startSparkCluster

object SparkStructuredStreamingMain extends IOApp.Simple {

  /** TODO
    * Pull images
    */
  def run: IO[Unit] = for {
    client         <- CatsDocker.client
    (zk, kafka)    <- startKafkaCluster(client, verboseWaitFor)
    (spark, slave) <- startSparkCluster(client, verboseWaitFor)
    _              <- race(toInterpret(client))(
                        List(spark, slave, kafka, zk).map(StopRequest.apply)
                      )
  } yield println("Started and stopped" + spark)

}
