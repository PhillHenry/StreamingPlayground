package uk.co.odinconsultants.sss
import org.apache.spark.sql.{SaveMode, SparkSession, functions as F}
import uk.co.odinconsultants.MinioUtils.*
import uk.co.odinconsultants.S3Utils.load_config
import uk.co.odinconsultants.sss.SSSUtils.{SPARK_BLOCK_PORT, SPARK_DRIVER_PORT, sparkS3Session}

/**
 * See https://medium.com/@dineshvarma.guduru/reading-and-writing-data-from-to-minio-using-spark-8371aefa96d2
 */
object SparkS3Main {

  def main(args: Array[String]): Unit = {
    SparkStructuredStreamingMain.createLocalDnsMapping()
    val session = load_config(
      sparkS3Session(URL_S3),
      MINIO_ROOT_USER,
      MINIO_ROOT_PASSWORD,
      URL_S3,
    )
    println("About to write....")
    val path  = s"s3a://$BUCKET_NAME/test_parquet"
    session.range(1000).write.mode(SaveMode.Overwrite).parquet(path)
    session.read.parquet(path).show()
//    session.range(1000).agg(F.mean(F.col("id"))).show()
  }

}
