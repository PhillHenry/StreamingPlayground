package uk.co.odinconsultants.sss
import org.apache.spark.sql.SaveMode
import uk.co.odinconsultants.sss.SparkStructuredStreamingMain.sparkS3Session

/**
 * See https://medium.com/@dineshvarma.guduru/reading-and-writing-data-from-to-minio-using-spark-8371aefa96d2
 */
object SparkS3Main {

  def main(args: Array[String]): Unit = {
    val session = sparkS3Session(args(0))
    println("About to write....")
    session.range(1000).write.mode(SaveMode.Overwrite).parquet("s3a://test2/test")
  }

}
