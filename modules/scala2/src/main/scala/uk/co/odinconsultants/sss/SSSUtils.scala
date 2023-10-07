package uk.co.odinconsultants.sss
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{DataFrame, SparkSession}
import uk.co.odinconsultants.S3Utils.{BUCKET_NAME, MINIO_ROOT_PASSWORD, MINIO_ROOT_USER, load_config}

object SSSUtils {

  val TOPIC_NAME                       = "test_topic"
  val BOOTSTRAP                        = "kafka_bootstrap"
  val OUTSIDE_KAFKA_BOOTSTRAP_PORT_INT = 9111
  val TIME_FORMATE                     = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"
  val SINK_PATH                        = s"s3a://$BUCKET_NAME/test"
  val SPARK_DRIVER_PORT                = 10027 // you'll need to open your firewall to this port
  val SPARK_BLOCK_PORT                 = 10028 // and this

  def sparkRead(endpoint: String): (StreamingQuery, DataFrame) = {
    import org.apache.spark.sql.streaming.{OutputMode, Trigger}

    val spark               = sparkS3Session(endpoint)
//    implicit val decoder    = org.apache.spark.sql.Encoders.STRING
//    implicit val ts_decoder = org.apache.spark.sql.Encoders.TIMESTAMP
    val df                  = spark.readStream
      .format("kafka")
      .option(
        "kafka.bootstrap.servers",
        s"127.0.0.1:$OUTSIDE_KAFKA_BOOTSTRAP_PORT_INT,$BOOTSTRAP:$OUTSIDE_KAFKA_BOOTSTRAP_PORT_INT",
      )
      .option("subscribe", TOPIC_NAME)
      .option("offset", "earliest")
      .option("startingOffsets", "earliest")
      .load()
    df.printSchema()
    val col_ts              = "ts"
    val partition           = "partition"
    val query2              = df
      .select(
        col("key"),
        to_timestamp(col("value").cast(StringType), TIME_FORMATE).alias(col_ts),
        col(partition),
      )
      .withWatermark(col_ts, "60 seconds")
      .groupBy(partition, col_ts)
      .agg(count("*"))
      .withWatermark(col_ts, "60 seconds")
      .writeStream
      .format("parquet")
      .outputMode(OutputMode.Append())
      .option("truncate", "false")
      .option("path", SINK_PATH)
      .option("checkpointLocation", SINK_PATH + "checkpoint")
      .trigger(Trigger.ProcessingTime(10000))
      .queryName("console")
      .start()
    query2.awaitTermination()
    (query2, df)
  }

  def sparkS3Session(endpoint: String): SparkSession =
    load_config(
      SparkSession.builder()
        .appName("HelloWorld")
        .master("spark://127.0.0.1:7077")
        .config("spark.driver.host", "172.17.0.1")
        .config("spark.driver.port", SPARK_DRIVER_PORT.toString)
        .config("spark.driver.blockManager.port", SPARK_BLOCK_PORT.toString)
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.1")
        .config(
          "spark.jars",
          s"${System.getProperty("user.home")}/.cache/coursier/v1/https/repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.1/hadoop-aws-3.3.1.jar,${System.getProperty("user.home")}/.cache/coursier/v1/https/repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.11.901/aws-java-sdk-bundle-1.11.901.jar",
        )
        .getOrCreate(),
      MINIO_ROOT_USER,
      MINIO_ROOT_PASSWORD,
      endpoint,
    )

}
