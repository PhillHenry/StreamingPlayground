package uk.co.odinconsultants.sss

import cats.effect.{IO, IOApp}
import fs2.Stream
import uk.co.odinconsultants.dreadnought.Flow.race
import uk.co.odinconsultants.dreadnought.docker.Algebra.toInterpret
import uk.co.odinconsultants.dreadnought.docker.Logging.verboseWaitFor
import uk.co.odinconsultants.dreadnought.docker.SparkStructuredStreamingMain.startSparkCluster
import uk.co.odinconsultants.dreadnought.docker.ZKKafkaMain.startKafkaCluster
import uk.co.odinconsultants.dreadnought.docker.*
import uk.co.odinconsultants.dreadnought.docker.KafkaAntics.produceMessages
import com.comcast.ip4s.*
import scala.concurrent.duration.*


object SparkStructuredStreamingMain extends IOApp.Simple {

  val TOPIC_NAME = "test_topic"

  /** TODO
    * Pull images
    */
  def run: IO[Unit] = for {
    client         <- CatsDocker.client
    (zk, kafka)    <- startKafkaCluster(client, verboseWaitFor)
    (spark, slave) <- startSparkCluster(client, verboseWaitFor)
    _              <- produceMessages(ip"127.0.0.1", port"9092", TOPIC_NAME)
                        .handleErrorWith(x => Stream.eval(IO(x.printStackTrace())))
                        .compile
                        .drain
    _              <- sparkRead
    _              <- race(toInterpret(client))(
                        List(spark, slave, kafka, zk).map(StopRequest.apply)
                      )
  } yield println("Started and stopped" + spark)

  val sparkRead = IO {
//    import org.apache.spark.sql.SparkSession
//    implicit val decoder = org.apache.spark.sql.Encoders.STRING
//    val spark = SparkSession.builder
//      .appName("HelloWorld")
//      .master("127.0.0.1:7077")
//      .getOrCreate()
//
//    val df = spark.readStream
//      .format("kafka")
//      .option("kafka.bootstrap.servers", "127.0.0.1:9092")
//      .option("subscribe", TOPIC_NAME)
//      .load()
//    df.selectExpr("CAST(value AS STRING)").as[String].show(10)
  }

}
