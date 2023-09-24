package uk.co.odinconsultants
import cats.effect.{Deferred, IO, IOApp}
import cats.free.Free
import fs2.Stream
import uk.co.odinconsultants.dreadnought.Flow.race
import uk.co.odinconsultants.dreadnought.docker.Algebra.toInterpret
import uk.co.odinconsultants.dreadnought.docker.Logging.{ioPrintln, verboseWaitFor}
import uk.co.odinconsultants.dreadnought.docker.*
import com.comcast.ip4s.*
import com.github.dockerjava.api.DockerClient
import fs2.kafka.ProducerSettings
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.streaming.StreamingQuery
import uk.co.odinconsultants.S3Utils.{
  ENDPOINT_S3,
  MINIO_ROOT_PASSWORD,
  MINIO_ROOT_USER,
  load_config,
}
import uk.co.odinconsultants.dreadnought.docker.CatsDocker.{createNetwork, interpret, removeNetwork}
import uk.co.odinconsultants.dreadnought.docker.KafkaAntics.createCustomTopic
import uk.co.odinconsultants.dreadnought.docker.KafkaRaft
import uk.co.odinconsultants.dreadnought.docker.Logging.{LoggingLatch, verboseWaitFor}
import com.comcast.ip4s.*

import scala.concurrent.duration.*

object SparkUtils {

  val BOOTSTRAP         = "kafka1"
  val SPARK_MASTER      = "spark-master"
  val SPARK_DRIVER_PORT = 10027 // you'll need to open your firewall to this port

  def startSparkWorker(
      client:      DockerClient,
      master:      ContainerId,
      slaveWait:   String => IO[Unit],
      master_node: String,
      networkName: String,
      s3_node:     String,
  ): IO[ContainerId] = CatsDocker.interpret(
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
                   List(
                     s"SPARK_MASTER=spark://${master_node}:7077",
                     s"SPARK_WORKER_OPTS=\"-Dspark.driver.host=172.17.0.1 -Dspark.driver.port=$SPARK_DRIVER_PORT\"",
                   ),
                   List.empty,
                   List(
                     s"$master"  -> SPARK_MASTER,
                     "kafka1"    -> BOOTSTRAP,
                     s"$s3_node" -> ENDPOINT_S3,
                   ),
                   networkName = Some(networkName),
                 )
               )
      _     <-
        Free.liftF(
          LoggingRequest(spark, slaveWait)
        )
    } yield spark,
  )

  def waitForMaster(
      client:       DockerClient,
      loggingLatch: LoggingLatch,
      timeout:      FiniteDuration,
      networkName:  String,
      dnsMappings:  DnsMapping[String],
  ): IO[ContainerId] = for {
    sparkLatch <- Deferred[IO, String]
    sparkWait   = loggingLatch("I have been elected leader! New state: ALIVE", sparkLatch)
    spark      <- startMaster(port"8082", port"7077", client, sparkWait, networkName, dnsMappings)
    _          <- IO.println("Waiting for Spark master to start...")
    _          <- sparkLatch.get.timeout(timeout)
    _          <- IO.println("Spark master started")
  } yield spark

  def startMaster(
      webPort:     Port,
      servicePort: Port,
      client:      DockerClient,
      logging:     String => IO[Unit],
      networkName: String,
      dnsMappings: DnsMapping[String],
  ): IO[ContainerId] = CatsDocker.interpret(
    client,
    for {
      spark <- Free.liftF(sparkMaster(webPort, servicePort, networkName, dnsMappings))
      _     <-
        Free.liftF(
          LoggingRequest(spark, logging)
        )
    } yield spark,
  )

  def sparkMaster(
      webPort:     Port,
      servicePort: Port,
      networkName: String,
      dnsMappings: DnsMapping[String],
  ): StartRequest =
    StartRequest(
      ImageName("ph1ll1phenry/spark_master_3_3_0_scala_2_13_hadoop_3"),
      Command("/bin/bash /master.sh"),
      List("INIT_DAEMON_STEP=setup_spark"),
      List(8080 -> webPort.value, 7077 -> servicePort.value),
      dnsMappings,
      networkName = Some(networkName),
      name = Some(SPARK_MASTER),
    )

  def sparkS3Session(endpoint: String): SparkSession =
    load_config(
      SparkSession.builder
        .appName("HelloWorld")
        .master("spark://127.0.0.1:7077")
        .config("spark.driver.host", "172.17.0.1")
        .config("spark.driver.port", SPARK_DRIVER_PORT)
        .getOrCreate(),
      MINIO_ROOT_USER,
      MINIO_ROOT_PASSWORD,
      endpoint,
    )
}
