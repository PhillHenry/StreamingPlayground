package uk.co.odinconsultants.sss

import cats.effect.{Deferred, IO, IOApp, Resource}
import cats.free.Free
import com.comcast.ip4s.*
import com.github.dockerjava.api.DockerClient
import fs2.Stream
import fs2.kafka.*
import io.minio.{MakeBucketArgs, MinioClient}
import org.apache.spark.sql.functions.*
import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.burningwave.tools.net.{DefaultHostResolver, HostResolutionRequestInterceptor, MappedHostResolver}
import uk.co.odinconsultants.MinioUtils
import uk.co.odinconsultants.MinioUtils.*
import uk.co.odinconsultants.SparkUtils.*
import uk.co.odinconsultants.dreadnought.Flow.race
import uk.co.odinconsultants.dreadnought.docker.Algebra.toInterpret
import uk.co.odinconsultants.dreadnought.docker.CatsDocker.{createNetwork, interpret, removeNetwork}
import uk.co.odinconsultants.dreadnought.docker.KafkaAntics.createCustomTopic
import uk.co.odinconsultants.dreadnought.docker.*
import uk.co.odinconsultants.dreadnought.docker.Logging.{LoggingLatch, ioPrintln, verboseWaitFor}
import uk.co.odinconsultants.kafka.KafkaUtils.startKafkas
import uk.co.odinconsultants.sss.SSSUtils.{BOOTSTRAP, OUTSIDE_KAFKA_BOOTSTRAP_PORT_INT, SINK_PATH, TIME_FORMATE, TOPIC_NAME, sparkRead}

import java.nio.file.Files
import java.text.SimpleDateFormat
import java.util.{Date, TimeZone}
import scala.collection.immutable.List
import scala.concurrent.duration.*

object SparkStructuredStreamingMain extends IOApp.Simple {

  val networkName                  = "my_network"
  val OUTSIDE_KAFKA_BOOTSTRAP_PORT = port"9111" // hard coded - not good

  def toNetworkName(x: String): String = x.replace("/", "")

  def toEndpoint(x: ContainerId): String = toEndpointName(x.toString)

  def toEndpointName(x: String): String = x.substring(0, 12)

  def ioLog(x: String): IO[Unit] = IO.println(s"PH ${new Date()}: $x")

  /** TODO
    * Pull images
    */
  def run: IO[Unit] = for {
    client         <- CatsDocker.client
    _              <- removeNetwork(client, networkName).handleErrorWith(x =>
                        ioLog(s"Did not delete network $networkName.\n${x.getMessage}")
                      )
    _              <- createNetwork(client, networkName)
    dir             = Files.createTempDirectory("PH").toString
    _              <- ioLog(s"directory = $dir")
    minio          <- startMinio(client, networkName, dir)
    kafkas         <- startKafkas(client, networkName)
    s3_node         = toEndpoint(minio)
    spark          <-
      waitForMaster(
        client,
        verboseWaitFor(Some(s"${Console.MAGENTA}SparkMaster: ")),
        30.seconds,
        networkName,
        List(s3_node -> ENDPOINT_S3),
      )
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
    _              <- ioLog(s"About to start slave with bootstrap mappings to $mappings...")
    master_node     = toEndpoint(spark)
    slave          <- startSparkWorker(client, spark, slaveWait, master_node, networkName, s3_node)
    _              <- slaveLatch.get.timeout(20.seconds)
    _               = createLocalDnsMapping(additional = Map(s3_node -> "127.0.0.1"))
    _              <- makeMinioBucket(URL_S3).handleErrorWith { (x: Throwable) =>
                        IO(x.printStackTrace()) *> ioLog(
                          "Could not create bucket but that's OK if it's already been created"
                        )
                      }
    _              <- ioLog("About to send messages")
    _              <- sendMessages
    _              <- ioLog("About to read messages")
    _              <-
      (//(sparkReadIO(s"http://$s3_node:9000/") *> ioLog("Finished reading messages")).start *>
        IO.sleep(10.seconds) *>
        (ioLog(
          "About to send some more messages"
        ) *>
          sendMessages *> IO.sleep(10.seconds)).foreverM.start *>
        ioLog("About to close down. Press return to end") *>
        IO.readLine).handleErrorWith(t => IO(t.printStackTrace()))
    _              <- race(toInterpret(client))(
                        (List(spark, slave, minio) ++ kafkas).map(StopRequest.apply)
                      )
  } yield println("Started and stopped" + spark)

  def createLocalDnsMapping(additional: Map[String, String] = Map.empty[String, String]): Unit = {
    import scala.jdk.CollectionConverters.*
    HostResolutionRequestInterceptor.INSTANCE.install(
      new MappedHostResolver((Map(ENDPOINT_S3 -> "127.0.0.1") ++ additional).asJava),
      DefaultHostResolver.INSTANCE,
    );
  }

  private val sendMessages: IO[Unit] = {
    val bootstrapServer                                        = s"localhost:${OUTSIDE_KAFKA_BOOTSTRAP_PORT}"
    val producerSettings: ProducerSettings[IO, String, String] =
      ProducerSettings[IO, String, String]
        .withBootstrapServers(bootstrapServer)
    TransactionalKafkaProducer
      .stream(
        TransactionalProducerSettings(
          s"transactionId${System.currentTimeMillis()}",
          producerSettings.withRetries(10),
        )
      )
      .flatMap { producer =>
        val messages: Stream[IO, ProducerResult[String, String]] =
          produceWithoutOffsets(producer, TOPIC_NAME)
        messages
      }
      .handleErrorWith(x =>
        Stream.eval(ioLog("Failed to send messages") *> IO(x.printStackTrace()))
      )
      .compile
      .drain
  }

  private def produceWithoutOffsets(
      producer: TransactionalKafkaProducer.WithoutOffsets[IO, String, String],
      topic:    String,
  ): Stream[IO, ProducerResult[String, String]] =
    createPureMessages(topic).evalMap { case record =>
      ioLog(s"buffering $record") *> producer.produceWithoutOffsets(record)
    }

  def createPureMessages(topic: String): Stream[IO, ProducerRecords[String, String]] = {
    val tz = TimeZone.getTimeZone("UTC")
    val df = new SimpleDateFormat(TIME_FORMATE)
    df.setTimeZone(tz)

    Stream
      .emits(List("a", "b", "c", "d").zipWithIndex)
      .evalTap(x => ioLog(s"Creating message $x"))
      .map((x, i) => ProducerRecords.one(ProducerRecord(topic, s"key_$x", df.format(new Date()))))
      .covary[IO]
  }

  def sparkReadIO(endpoint: String): IO[(StreamingQuery, DataFrame)] = IO(sparkRead(endpoint))

}
