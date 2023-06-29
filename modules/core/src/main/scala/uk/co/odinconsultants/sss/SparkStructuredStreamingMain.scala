package uk.co.odinconsultants.sss

import cats.effect.{Deferred, IO, IOApp}
import cats.free.Free
import fs2.Stream
import uk.co.odinconsultants.dreadnought.Flow.race
import uk.co.odinconsultants.dreadnought.docker.Algebra.toInterpret
import uk.co.odinconsultants.dreadnought.docker.Logging.verboseWaitFor
import uk.co.odinconsultants.dreadnought.docker.SparkStructuredStreamingMain.{
  startSlave,
  startSparkCluster,
  waitForMaster,
}
import uk.co.odinconsultants.dreadnought.docker.ZKKafkaMain.startKafkaCluster
import uk.co.odinconsultants.dreadnought.docker.*
import uk.co.odinconsultants.dreadnought.docker.KafkaAntics.produceMessages
import com.comcast.ip4s.*

import scala.concurrent.duration.*

object SparkStructuredStreamingMain extends IOApp.Simple {

  val TOPIC_NAME = "test_topic"
  val BOOTSTRAP  = "kafka_bootstrap"
  val BROKER     = "zk"

  /** TODO
    * Pull images
    */
  def run: IO[Unit] = for {
    client      <- CatsDocker.client
    (zk, kafka) <- startKafkaCluster(client, verboseWaitFor, 20.seconds)
    spark       <- waitForMaster(client, verboseWaitFor, 20.seconds)
    masterName  <- CatsDocker.interpret(client, Free.liftF(NamesRequest(spark)))
    kafkaName   <- CatsDocker.interpret(client, Free.liftF(NamesRequest(kafka)))
    zkName      <- CatsDocker.interpret(client, Free.liftF(NamesRequest(zk)))
    slaveLatch  <- Deferred[IO, String]
    slaveWait    = verboseWaitFor("Successfully registered with master", slaveLatch)

    slave <- CatsDocker.interpret(
               client,
               for {
                 // the problem with this is that the worker tries to make connections to the JVM running this.
                 // You'll see this in the workers logs:
                 // 23/06/29 08:45:06 WARN NettyRpcEnv: Ignored failure: java.io.IOException: Connecting to adele.lan/192.168.1.147:32851 timed out (120000 ms)
                 // that port (32851) refers to this JVM. You can do this but you need to reconfigure the container.
                 // See:
                 // https://github.com/jenkinsci/docker-plugin/issues/893
                 // https://docs.docker.com/network/drivers/bridge/
                 // and withNetworkMode in com/github/dockerjava/api/model/HostConfig.java
                 spark <- Free.liftF(
                            StartRequest(
                              ImageName("ph1ll1phenry/spark_worker_3_3_0_scala_2_13_hadoop_3"),
                              Command("/bin/bash /worker.sh"),
                              List(s"SPARK_MASTER=spark://spark-master:7077"),
                              List.empty,
                              masterName.map(_ -> "spark-master")
                                ++ kafkaName.map(_ -> BOOTSTRAP)
                                ++ zkName.map(_ -> BROKER),
//                               :+ ("host.docker.internal" -> "172.17.0.1")
                            )
                          )
                 _     <-
                   Free.liftF(
                     LoggingRequest(spark, slaveWait)
                   )
               } yield spark,
             )
    _     <- slaveLatch.get.timeout(20.seconds)
    _     <- sendMessages
    _     <- sparkRead
    _     <- sendMessages
    _     <- race(toInterpret(client))(
               List(spark, slave, kafka, zk).map(StopRequest.apply)
             )
  } yield println("Started and stopped" + spark)

  private val sendMessages =
    produceMessages(ip"127.0.0.1", port"9092", TOPIC_NAME)
      .handleErrorWith(x => Stream.eval(IO(x.printStackTrace())))
      .compile
      .drain
    
  val sparkRead = IO {
    import org.apache.spark.sql.SparkSession

    val spark = SparkSession.builder
      .appName("HelloWorld")
      .master("spark://127.0.0.1:7077")
      .getOrCreate()

    implicit val decoder = org.apache.spark.sql.Encoders.STRING
    val df               = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", s"$BROKER:9092,127.0.0.1:9092,$BOOTSTRAP:9092")
      .option("subscribe", "test_topic")
      .option("startingOffsets", "earliest")
      .option("startingOffsets", "earliest")
      .load()
//    df.selectExpr("CAST(value AS STRING)").as[String].show(10)
    val query2           = df
      .selectExpr("CAST(value AS STRING)")
      .as[String]
      .writeStream
      .format("console")
      .start()
    query2.awaitTermination(10000)
  }

}
