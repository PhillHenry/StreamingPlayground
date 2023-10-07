package uk.co.odinconsultants

import com.amazonaws.SDKGlobalConfiguration
import org.apache.spark.sql.SparkSession

object S3Utils {

  val MINIO_ROOT_USER     = "minio-root-user"
  val MINIO_ROOT_PASSWORD = "minio-root-password"
  val BUCKET_NAME         = "mybucket"
  val ENDPOINT_S3         = "mys3"
  val URL_S3              = s"http://$ENDPOINT_S3:9000/"

  /** @param access_key AWS_ACCESS_KEY_ID
    * @param secret_key AWS_SECRET_ACCESS_KEY
    * @param endpoint ENDPOINT
    */
  def load_config(
      session:    SparkSession,
      access_key: String,
      secret_key: String,
      endpoint:   String,
  ): SparkSession = {
    System.setProperty(SDKGlobalConfiguration.DISABLE_CERT_CHECKING_SYSTEM_PROPERTY, "true")
    println(s"Endpoint is $endpoint")
    val conf = session.sparkContext.hadoopConfiguration
    conf.set("fs.s3a.access.key", access_key)
    conf.set("fs.s3a.secret.key", secret_key)
    conf.set("fs.s3a.endpoint", endpoint)
    conf.set("fs.s3a.connection.ssl.enabled", "true")
    conf.set("fs.s3a.path.style.access", "true")
    conf.set("fs.s3a.attempts.maximum", "1")
    conf.set("fs.s3a.connection.establish.timeout", "5000")
    conf.set("fs.s3a.connection.timeout", "10000")
    conf.set("fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    session
  }


}
