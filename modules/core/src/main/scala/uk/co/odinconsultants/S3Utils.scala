package uk.co.odinconsultants
import cats.effect.IO
import cats.free.Free
import com.amazonaws.SDKGlobalConfiguration
import com.github.dockerjava.api.DockerClient
import io.minio.{MakeBucketArgs, MinioClient}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import uk.co.odinconsultants.dreadnought.docker.{
  CatsDocker,
  Command,
  ContainerId,
  ImageName,
  StartRequest,
}

import java.nio.file.{FileSystems, Files, Path}

object S3Utils {

  val MINIO_ROOT_USER     = "minio-root-user"
  val MINIO_ROOT_PASSWORD = "minio-root-password"
  val BUCKET_NAME         = "mybucket"
  val ENDPOINT_S3         = "mys3"

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

  def startMinio(client: DockerClient, networkName: String, dir: String): IO[ContainerId] =
    IO(Files.createDirectories(FileSystems.getDefault().getPath(dir))).handleErrorWith(t =>
      IO.println(s"Could not create $dir")
    ) *>
      CatsDocker.interpret(
        client,
        for {
          minio <- Free.liftF(
                     StartRequest(
                       ImageName("bitnami/minio:latest"),
                       Command("/opt/bitnami/scripts/minio/run.sh"),
                       List(
                         s"MINIO_ROOT_USER=$MINIO_ROOT_USER",
                         s"MINIO_ROOT_PASSWORD=$MINIO_ROOT_PASSWORD",
                       ),
                       List(9000 -> 9000),
                       List.empty,
                       networkName = Some(networkName),
                       volumes = List((dir, "/bitnami/minio/data")),
                     )
                   )
        } yield minio,
      )

  def makeMinioBucket(endpoint: String): IO[Unit] = IO {
    val minioClient = MinioClient.builder
      .endpoint(endpoint)
      .credentials(MINIO_ROOT_USER, MINIO_ROOT_PASSWORD)
      .build
    minioClient.makeBucket(MakeBucketArgs.builder().bucket(BUCKET_NAME).build())
  }

}
