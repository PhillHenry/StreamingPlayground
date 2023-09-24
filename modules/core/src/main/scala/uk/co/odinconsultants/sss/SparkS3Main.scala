package uk.co.odinconsultants.sss
import org.apache.spark.sql.{SaveMode, SparkSession}
import uk.co.odinconsultants.S3Utils.{BUCKET_NAME, MINIO_ROOT_PASSWORD, MINIO_ROOT_USER, URL_S3, load_config}
import uk.co.odinconsultants.SparkUtils.{SPARK_DRIVER_PORT, sparkS3Session}

/**
 * See https://medium.com/@dineshvarma.guduru/reading-and-writing-data-from-to-minio-using-spark-8371aefa96d2
 */
object SparkS3Main {

  def main(args: Array[String]): Unit = {
    SparkStructuredStreamingMain.createLocalDnsMapping()
    val session = load_config(
      SparkSession.builder
        .appName("local")
        .master("local")
        .getOrCreate(),
      MINIO_ROOT_USER,
      MINIO_ROOT_PASSWORD,
      URL_S3,
    )
    println("About to write....")
    val path  = s"s3a://$BUCKET_NAME/test_parquet"
    session.range(1000).write.mode(SaveMode.Overwrite).parquet(path)
  }

}
