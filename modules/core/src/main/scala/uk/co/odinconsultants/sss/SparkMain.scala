package uk.co.odinconsultants.sss

import cats.effect.{IO, IOApp}
import com.comcast.ip4s.*
import fs2.Stream
import uk.co.odinconsultants.dreadnought.Flow.race
import uk.co.odinconsultants.dreadnought.docker.*
import uk.co.odinconsultants.dreadnought.docker.Algebra.toInterpret
import uk.co.odinconsultants.dreadnought.docker.KafkaAntics.produceMessages
import uk.co.odinconsultants.dreadnought.docker.Logging.verboseWaitFor
import uk.co.odinconsultants.dreadnought.docker.SparkStructuredStreamingMain.startSparkCluster
import uk.co.odinconsultants.dreadnought.docker.ZKKafkaMain.startKafkaCluster
import uk.co.odinconsultants.sss.SparkStructuredStreamingMain.sparkRead

import scala.concurrent.duration.*

object SparkMain extends IOApp.Simple {

  /** TODO
    * Pull images
    */
  def run: IO[Unit] = for {
    _              <- sparkRead
  } yield println("Connected to Spark")

}
