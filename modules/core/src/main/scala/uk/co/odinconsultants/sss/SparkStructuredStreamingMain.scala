package uk.co.odinconsultants.sss

import cats.effect.{Deferred, IO, IOApp}
import cats.free.Free
import fs2.Stream
import uk.co.odinconsultants.dreadnought.Flow.race
import uk.co.odinconsultants.dreadnought.docker.Algebra.toInterpret
import uk.co.odinconsultants.dreadnought.docker.Logging.{ioPrintln, verboseWaitFor}
import uk.co.odinconsultants.dreadnought.docker.*
import uk.co.odinconsultants.dreadnought.docker.KafkaAntics.produceMessages
import com.comcast.ip4s.*
import com.github.dockerjava.api.DockerClient
import uk.co.odinconsultants.dreadnought.docker.CatsDocker.{createNetwork, interpret, removeNetwork}
import uk.co.odinconsultants.dreadnought.docker.KafkaRaft
import uk.co.odinconsultants.dreadnought.docker.Logging.{LoggingLatch, verboseWaitFor}

import scala.collection.immutable.List
import scala.concurrent.duration.*

object SparkStructuredStreamingMain extends IOApp.Simple {

  val TOPIC_NAME                   = "test_topic"
  val BOOTSTRAP                    = "kafka1"
  val networkName                  = "my_network"
  val SPARK_MASTER                 = "spark-master"
  val OUTSIDE_KAFKA_BOOTSTRAP_PORT = port"9111"

  def toNetworkName(x: String): String = x.replace("/", "")

  /** TODO
    * Pull images
    */
  def run: IO[Unit] = for {
    client         <- CatsDocker.client
    _              <- removeNetwork(client, networkName).handleErrorWith(x =>
                        IO.println(s"Did not delete network $networkName.\n${x.getMessage}")
                      )
    _              <- createNetwork(client, networkName)
    kafkaStart     <- Deferred[IO, String]
    kafkaLatch      =
      verboseWaitFor(Some(s"${Console.BLUE}kafka1: "))("started (kafka.server.Kafka", kafkaStart)
    loggers         = List(
                        kafkaLatch,
                        ioPrintln(Some(s"${Console.GREEN}kafka2: ")),
                        ioPrintln(Some(s"${Console.YELLOW}kafka3: ")),
                      )
    kafkas         <-
      interpret(
        client,
        KafkaRaft.startKafkas(loggers, networkName),
      )
    spark          <-
      waitForMaster(client, verboseWaitFor(Some(s"${Console.MAGENTA}SparkMaster: ")), 30.seconds)
    masterName     <- CatsDocker.interpret(client, Free.liftF(NamesRequest(spark)))
    slaveLatch     <- Deferred[IO, String]
    slaveWait       = verboseWaitFor(Some(s"${Console.RED}SparkSlave: "))(
                        "Successfully registered with master",
                        slaveLatch,
                      )
    bootstrapNames <- interpret(client, Free.liftF(NamesRequest(kafkas.head)))
    mappings        =
      masterName.map((x: String) => toNetworkName(x) -> SPARK_MASTER) ++ bootstrapNames.map(
        (x: String) => toNetworkName(x) -> BOOTSTRAP
      )
    _              <- IO.println(s"About to start slave with bootstrap mappings to $mappings...")
    slave          <- CatsDocker.interpret(
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
                                       List(s"SPARK_MASTER=spark://${spark.toString.substring(0, 12)}:7077"),
                                       List.empty,
                                       List(s"$spark" -> SPARK_MASTER, "kafka1" -> BOOTSTRAP),
                                       networkName = Some(networkName),
                                     )
                                   )
                          _     <-
                            Free.liftF(
                              LoggingRequest(spark, slaveWait)
                            )
                        } yield spark,
                      )
    _              <- slaveLatch.get.timeout(20.seconds)
    _              <- sendMessages
    _              <- sparkRead
    _              <- sendMessages
    _              <- race(toInterpret(client))(
                        (List(spark, slave) ++ kafkas).map(StopRequest.apply)
                      )
  } yield println("Started and stopped" + spark)

  private val sendMessages =
    produceMessages(ip"127.0.0.1", OUTSIDE_KAFKA_BOOTSTRAP_PORT, TOPIC_NAME)
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
      .option(
        "kafka.bootstrap.servers",
        s"127.0.0.1:$OUTSIDE_KAFKA_BOOTSTRAP_PORT,$BOOTSTRAP:$OUTSIDE_KAFKA_BOOTSTRAP_PORT",
      )
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

  def waitForMaster(
      client:       DockerClient,
      loggingLatch: LoggingLatch,
      timeout:      FiniteDuration,
  ): IO[ContainerId] = for {
    sparkLatch <- Deferred[IO, String]
    sparkWait   = loggingLatch("I have been elected leader! New state: ALIVE", sparkLatch)
    spark      <- startMaster(port"8082", port"7077", client, sparkWait)
    _          <- IO.println("Waiting for Spark master to start...")
    _          <- sparkLatch.get.timeout(timeout)
    _          <- IO.println("Spark master started")
  } yield spark

  def startMaster(
      webPort:     Port,
      servicePort: Port,
      client:      DockerClient,
      logging:     String => IO[Unit],
  ): IO[ContainerId] = CatsDocker.interpret(
    client,
    for {
      spark <- Free.liftF(sparkMaster(webPort, servicePort))
      _     <-
        Free.liftF(
          LoggingRequest(spark, logging)
        )
    } yield spark,
  )

  def sparkMaster(webPort: Port, servicePort: Port): StartRequest = StartRequest(
    ImageName("ph1ll1phenry/spark_master_3_3_0_scala_2_13_hadoop_3"),
    Command("/bin/bash /master.sh"),
    List("INIT_DAEMON_STEP=setup_spark"),
    List(8080 -> webPort.value, 7077 -> servicePort.value),
    List.empty,
    networkName = Some(networkName),
    name = Some(SPARK_MASTER),
  )
}
