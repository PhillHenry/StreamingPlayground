package uk.co.odinconsultants.sss

import cats.effect.kernel.Ref
import cats.effect.{Deferred, IO, IOApp, Ref, Resource}
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
import org.burningwave.tools.net.{
  DefaultHostResolver,
  HostResolutionRequestInterceptor,
  MappedHostResolver,
}
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
import uk.co.odinconsultants.sss.SSSUtils.{
  BOOTSTRAP,
  KEY,
  MAX_EXECUTORS,
  OUTSIDE_KAFKA_BOOTSTRAP_PORT_INT,
  SINK_PATH,
  TIMESTAMP_COL,
  TIME_FORMATE,
  TOPIC_NAME,
  WATERMARK_SECONDS,
  sparkRead,
  sparkS3Session,
}

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

  def ioLog(x: String): IO[Unit] = IO.println(toMessage(x))

  def toMessage(x: => String): String = s"PH ${new Date()}: $x"

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
    kafkas         <- startKafkas(client, networkName, MAX_EXECUTORS / 2)
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
    counter        <- Ref.of[IO, Int](0)
    _              <- sendMessages(counter)
    _              <- ioLog("About to read messages")
    s3_endpoint     = s"http://$s3_node:9000/"
    _              <- ioLog("About to start polling S3") *> pollS3(s3_endpoint, counter).handleErrorWith(t =>
                        ioLog("Could not start polling S3") *> IO(t.printStackTrace())
                      )
    _              <-
      ((sparkReadIO(s3_endpoint) *> ioLog("Finished reading messages")).start *>
        IO.sleep((WATERMARK_SECONDS / 2).seconds) *>
        sendMessagesPauseThenSendMore(counter) *>
        ioLog("About to close down. Press return to end") *>
        IO.readLine).handleErrorWith(t => IO(t.printStackTrace()))
    _              <- race(toInterpret(client))(
                        (List(spark, slave, minio) ++ kafkas).map(StopRequest.apply)
                      )
  } yield println("Started and stopped" + spark)

  def sendMessagesPauseThenSendMore(counter: Ref[IO, Int]) = (ioLog("First batch") *>
    sendMessageBatchs(5, counter) *>
    IO.sleep((WATERMARK_SECONDS * 2).seconds) *>
    ioLog("Second batch") *>
    sendMessageBatchs(5, counter) *>
    ioLog("Finished sending")).start

  def sendMessageBatchs(batches: Int, counter: Ref[IO, Int]) = (sendMessages(counter) *>
    IO.sleep((WATERMARK_SECONDS / 2).seconds)).replicateA_(batches)

  def pollS3(s3_endpoint: String, counter: Ref[IO, Int]): IO[Unit] = for {
    _     <- ioLog("About to get Spark session")
    spark <- IO(sparkS3Session(s3_endpoint, "query"))
    _     <- ioLog("Have Spark session")
    sent  <- counter.getAndUpdate(identity)
    _     <- (IO {
               val latest = spark.read.parquet(SINK_PATH).agg(max(TIMESTAMP_COL)).collect()
               val ids    = spark.read
                 .parquet(SINK_PATH)
                 .select(KEY)
                 .collect()
                 .map(x => new String(x.getAs[Array[Byte]](0)).toInt)
                 .sorted
               println(
                 toMessage(
                   s"${ids.size}/$sent events written to S3. Most recent persisted timestamp: ${latest(0)}, ids = ${ids
                       .mkString(", ")}"
                 )
               )
             }.handleErrorWith(t =>
               ioLog(s"Could not query $SINK_PATH ${t.getMessage}") *> IO(t.printStackTrace())
             ) *>
               IO.sleep((WATERMARK_SECONDS / 2).seconds)).foreverM.start
  } yield {}

  def createLocalDnsMapping(additional: Map[String, String] = Map.empty[String, String]): Unit = {
    import scala.jdk.CollectionConverters.*
    HostResolutionRequestInterceptor.INSTANCE.install(
      new MappedHostResolver((Map(ENDPOINT_S3 -> "127.0.0.1") ++ additional).asJava),
      DefaultHostResolver.INSTANCE,
    );
  }

  def sendMessages(counter: Ref[IO, Int]): IO[Unit] = {
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
          produceWithoutOffsets(producer, TOPIC_NAME, counter)
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
      counter:  Ref[IO, Int],
  ): Stream[IO, ProducerResult[String, String]] = {
    val tz = TimeZone.getTimeZone("UTC")
    val df = new SimpleDateFormat(TIME_FORMATE)
    df.setTimeZone(tz)
    Stream.eval(counter.getAndUpdate(_ + 1)).evalMap { case i: Int =>
      ioLog(s"Sending message $i") *>
        producer.produceWithoutOffsets(
          ProducerRecords.one(ProducerRecord(topic, s"$i", df.format(new Date())))
        )
    }
  }

  def sparkReadIO(endpoint: String): IO[SparkSession] = IO(sparkRead(endpoint))

}
