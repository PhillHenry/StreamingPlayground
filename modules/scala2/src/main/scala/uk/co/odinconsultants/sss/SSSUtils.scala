package uk.co.odinconsultants.sss
import org.apache.spark.sql.functions._
//import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.SparkSession
import org.burningwave.tools.net.{
  DefaultHostResolver,
  HostResolutionRequestInterceptor,
  MappedHostResolver,
}
import uk.co.odinconsultants.S3Utils.{
  BUCKET_NAME,
  MINIO_ROOT_PASSWORD,
  MINIO_ROOT_USER,
  load_config,
}

object SSSUtils {

  val TOPIC_NAME                       = "test_topic"
  val BOOTSTRAP                        = "kafka1"
  val OUTSIDE_KAFKA_BOOTSTRAP_PORT_INT = 9091
  val TIME_FORMATE                     = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"
  val SINK_PATH                        = s"s3a://$BUCKET_NAME/test"
  val SPARK_DRIVER_PORT                = 10027 // you'll need to open your firewall to this port
  val WATERMARK_SECONDS                = 20
  val MAX_EXECUTORS                    = 4
  val TIMESTAMP_COL                    = "timestamp"
  val KEY                              = "key"
  val SPARK_BLOCK_PORT                 =
    10028 // and this. Actually, if you have 2 jos running, open 2 more monotonically rising

  def sparkRead(endpoint: String): SparkSession = {
    import org.apache.spark.sql.streaming.{OutputMode, Trigger}

    val spark = sparkS3Session(endpoint)
//    implicit val decoder    = org.apache.spark.sql.Encoders.STRING
//    implicit val ts_decoder = org.apache.spark.sql.Encoders.TIMESTAMP
    val df    = spark.readStream
      .format("kafka")
      .option(
        "kafka.bootstrap.servers",
        s"$BOOTSTRAP:$OUTSIDE_KAFKA_BOOTSTRAP_PORT_INT,localhost:${OUTSIDE_KAFKA_BOOTSTRAP_PORT_INT + 20}",
      )
      .option("subscribe", TOPIC_NAME)
      .option("offset", "earliest")
      .option("startingOffsets", "earliest")
      .load()
    df.printSchema()

    val partition = "partition"

    val query2 = df
      .select(
        col(KEY),
        col(TIMESTAMP_COL),
        col(partition),
      )
      .withWatermark(TIMESTAMP_COL, s"$WATERMARK_SECONDS seconds")
      .groupBy(partition, TIMESTAMP_COL, KEY)
      .agg(count("*"))
      .writeStream
      .format("parquet")
      .outputMode(OutputMode.Append()) // Update produces nothing when the groupBy is unique
      .option("truncate", "false")
      .option("path", SINK_PATH)
      .option("checkpointLocation", SINK_PATH + "checkpoint")
      .trigger(Trigger.ProcessingTime(WATERMARK_SECONDS * 1000L / 2))
      .queryName("console")
      .start()
    query2.awaitTermination()
    spark
  }

  def sparkS3Session(endpoint: String, appName: String = "HelloWorld"): SparkSession = {
    val home: String = System.getProperty("user.home")
    load_config(
      SparkSession
        .builder()
        .appName(appName)
        .master("spark://127.0.0.1:7077")
        .config("spark.driver.host", "172.17.0.1")
        .config("spark.dynamicAllocation.enabled", "true")
        .config("spark.dynamicAllocation.minExecutors", "1")
        .config("spark.dynamicAllocation.maxExecutors", s"$MAX_EXECUTORS")
        .config("spark.dynamicAllocation.shuffleTracking.enabled", "true")
        .config("spark.driver.port", SPARK_DRIVER_PORT.toString)
        .config("spark.driver.blockManager.port", SPARK_BLOCK_PORT.toString)
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.1")
        .config(
          "spark.jars",
          s"$home/.m2/repository/org/apache/spark/spark-sql-kafka-0-10_2.13/3.3.0/spark-sql-kafka-0-10_2.13-3.3.0.jar,$home/.m2/repository/org/apache/spark/spark-token-provider-kafka-0-10_2.13/3.3.0/spark-token-provider-kafka-0-10_2.13-3.3.0.jar,$home/.m2/repository/org/apache/kafka/kafka-clients/2.8.1/kafka-clients-2.8.1.jar,$home/.m2/repository/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar,$home/.m2/repository/org/apache/hadoop/hadoop-aws/3.3.1/hadoop-aws-3.3.1.jar,$home/.m2/repository/com/amazonaws/aws-java-sdk-bundle/1.11.901/aws-java-sdk-bundle-1.11.901.jar",
        )
        .getOrCreate(),
      MINIO_ROOT_USER,
      MINIO_ROOT_PASSWORD,
      endpoint,
    )
  }

  def main(args: Array[String]): Unit = {
    val container_id = "7cc35abc0817"
    import scala.jdk.CollectionConverters._
    HostResolutionRequestInterceptor.INSTANCE.install(
      new MappedHostResolver(Map(container_id -> "127.0.0.1").asJava),
      DefaultHostResolver.INSTANCE,
    );
    println(sparkRead("http://" + container_id + ":9000/"))
  }

}
