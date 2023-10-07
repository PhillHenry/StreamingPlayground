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

object MinioUtils {

  val MINIO_ROOT_USER     = "minio-root-user"
  val MINIO_ROOT_PASSWORD = "minio-root-password"
  val BUCKET_NAME         = "mybucket"
  val ENDPOINT_S3         = "mys3"
  val URL_S3              = s"http://$ENDPOINT_S3:9000/"

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
                       List(9000 -> 9000, 9001 -> 9001),
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
