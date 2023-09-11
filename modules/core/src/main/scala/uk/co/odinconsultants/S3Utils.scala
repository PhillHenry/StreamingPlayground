package uk.co.odinconsultants
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

object S3Utils {
  /**
   *
   * @param access_key AWS_ACCESS_KEY_ID
   * @param secret_key AWS_SECRET_ACCESS_KEY
   * @param endpoint ENDPOINT
   */
  def load_config(session: SparkSession, access_key: String, secret_key: String, endpoint: String): SparkSession = {
    val conf = session.sparkContext.hadoopConfiguration
    conf.set("fs.s3a.access.key", access_key)
    conf.set("fs.s3a.secret.key", secret_key)
    conf.set("fs.s3a.endpoint", endpoint)
    conf.set("fs.s3a.connection.ssl.enabled", "true")
    conf.set("fs.s3a.path.style.access", "true")
    conf.set("fs.s3a.attempts.maximum", "1")
    conf.set("fs.s3a.connection.establish.timeout", "5000")
    conf.set("fs.s3a.connection.timeout", "10000")
    return session
  }

}
