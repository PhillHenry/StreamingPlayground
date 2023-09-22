package uk.co.odinconsultants.sss

import cats.effect.{Deferred, IO, IOApp}
import cats.free.Free
import fs2.Stream
import uk.co.odinconsultants.dreadnought.Flow.race
import uk.co.odinconsultants.dreadnought.docker.Algebra.toInterpret
import uk.co.odinconsultants.dreadnought.docker.Logging.{ioPrintln, verboseWaitFor}
import uk.co.odinconsultants.dreadnought.docker.*
import com.comcast.ip4s.*
import com.github.dockerjava.api.DockerClient
import fs2.kafka.{ProducerRecord, ProducerRecords, ProducerResult, ProducerSettings, TransactionalKafkaProducer, TransactionalProducerSettings}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.functions.*
import uk.co.odinconsultants.dreadnought.docker.CatsDocker.{createNetwork, interpret, removeNetwork}
import uk.co.odinconsultants.dreadnought.docker.KafkaAntics.createCustomTopic
import uk.co.odinconsultants.dreadnought.docker.KafkaRaft
import uk.co.odinconsultants.dreadnought.docker.Logging.{LoggingLatch, verboseWaitFor}
import uk.co.odinconsultants.S3Utils
import io.minio.{MakeBucketArgs, MinioClient}
import uk.co.odinconsultants.S3Utils.{BUCKET_NAME, MINIO_ROOT_PASSWORD, MINIO_ROOT_USER, load_config, makeMinioBucket, startMinio}
import uk.co.odinconsultants.SparkUtils.{BOOTSTRAP, SPARK_DRIVER_PORT, SPARK_MASTER, sparkS3Session, startSparkWorker, waitForMaster}

import java.util.{Date, TimeZone}
import scala.collection.immutable.List
import scala.concurrent.duration.*
import cats.effect.Resource
import fs2.kafka.{ValueDeserializer, ValueSerializer}
import org.apache.spark.sql.types.StringType

import java.text.SimpleDateFormat

object SparkStructuredStreamingMain extends IOApp.Simple {

  val TOPIC_NAME                   = "test_topic"
  val networkName                  = "my_network"
  val OUTSIDE_KAFKA_BOOTSTRAP_PORT = port"9111" // hard coded - not good

  def toNetworkName(x: String): String = x.replace("/", "")

  def toEndpoint(x: ContainerId): String = toEndpointName(x.toString)

  def toEndpointName(x: String): String = x.substring(0, 12)

  /** TODO
    * Pull images
    */
  def run: IO[Unit] = for {
    client <- CatsDocker.client
    _      <- removeNetwork(client, networkName).handleErrorWith(x =>
                IO.println(s"Did not delete network $networkName.\n${x.getMessage}")
              )
    _      <- createNetwork(client, networkName)

    minio          <- startMinio(client, networkName)
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
    _              <- kafkaStart.get.timeout(20.seconds)
    _              <- IO.println(s"About to create topic $TOPIC_NAME")
    _              <- IO(createCustomTopic(TOPIC_NAME, OUTSIDE_KAFKA_BOOTSTRAP_PORT))
    spark          <-
      waitForMaster(
        client,
        verboseWaitFor(Some(s"${Console.MAGENTA}SparkMaster: ")),
        30.seconds,
        networkName,
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
    _              <- IO.println(s"About to start slave with bootstrap mappings to $mappings...")
    master_node     = toEndpoint(spark)
    slave          <- startSparkWorker(client, spark, slaveWait, master_node, networkName)
    _              <- slaveLatch.get.timeout(20.seconds)
    endpoint        = "http://localhost:9000/"
    _              <- makeMinioBucket(endpoint).handleErrorWith { (x: Throwable) =>
      IO(x.printStackTrace()) *> IO.println(
      "Could not create bucket but that's OK if it's already been created"
      )
    }
    _              <- IO.println("About to send messages")
    _              <- sendMessages
    _              <- IO.println("About to read messages")
    _              <-
      (sparkRead(endpoint) *>
        IO.sleep(10.seconds) *>
        IO.println(
          "About to send some more messages"
        ) *>
        sendMessages *>
        IO.println("About to close down. Press return to end") *>
        IO.readLine).handleErrorWith(t => IO(t.printStackTrace()))
    _              <- race(toInterpret(client))(
                        (List(spark, slave, minio) ++ kafkas).map(StopRequest.apply)
                      )
  } yield println("Started and stopped" + spark)

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
      .handleErrorWith(x => Stream.eval(IO(x.printStackTrace())))
      .compile
      .drain
  }

  private def produceWithoutOffsets(
      producer: TransactionalKafkaProducer.WithoutOffsets[IO, String, String],
      topic:    String,
  ): Stream[IO, ProducerResult[String, String]] =
    createPureMessages(topic).evalMap { case record =>
      IO.println(s"buffering $record") *> producer.produceWithoutOffsets(record)
    }

  def createPureMessages(topic: String): Stream[IO, ProducerRecords[String, String]] = {
    val tz = TimeZone.getTimeZone("UTC")
    val df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z") // Quoted "Z" to indicate UTC, no

    df.setTimeZone(tz)
    Stream
      .emits(List("a", "b", "c", "d").zipWithIndex)
      .evalTap(x => IO.println(s"Creating message $x"))
      .map((x, i) => ProducerRecords.one(ProducerRecord(topic, s"key_$x", df.format(new Date()))))
      .covary[IO]
  }

  def sparkRead(endpoint: String): IO[(StreamingQuery, DataFrame)] = IO {
    import org.apache.spark.sql.SparkSession
    import org.apache.spark.sql.streaming.{DataStreamWriter, OutputMode, Trigger}

    val spark = sparkS3Session(endpoint)

    implicit val decoder    = org.apache.spark.sql.Encoders.STRING
    implicit val ts_decoder = org.apache.spark.sql.Encoders.TIMESTAMP
    val df                  = spark.readStream
      .format("kafka")
      .option(
        "kafka.bootstrap.servers",
        s"127.0.0.1:$OUTSIDE_KAFKA_BOOTSTRAP_PORT,$BOOTSTRAP:$OUTSIDE_KAFKA_BOOTSTRAP_PORT",
      )
      .option("subscribe", TOPIC_NAME)
      .option("offset", "earliest")
      .option("startingOffsets", "earliest")
      .load()
    val path                = s"s3a://$BUCKET_NAME/test"
    df.printSchema()
    val col_ts     = "ts"
    val query2 = df.select(col("key"), to_timestamp(col("value").cast(StringType), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'").alias(col_ts))
      .withWatermark(col_ts, "60 seconds")
      .writeStream
      .format("parquet")
      .outputMode(OutputMode.Append())
      .option("truncate", "false")
      .option("path", path)
      .option("checkpointLocation", path + "checkpoint")
      .trigger(Trigger.ProcessingTime(10000))
      .queryName("console")
      .start()
    query2.awaitTermination(20000)
    (query2, df)
  }

}
