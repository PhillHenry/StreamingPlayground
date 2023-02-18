package uk.co.odinconsultants.sss

import cats.effect.{IO, Resource}
import org.apache.kafka.clients.admin.{AdminClient, AdminClientConfig}

import scala.concurrent.duration.FiniteDuration
import scala.jdk.CollectionConverters.*
import scala.concurrent.duration.*

object KafkaInitialisation {

  val startClient: IO[AdminClient] = IO {
    AdminClient.create(
      Map[String, Object](
        AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG       -> "127.0.0.1:9092",
        AdminClientConfig.CLIENT_ID_CONFIG               -> "test-kafka-admin-client",
        AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG      -> "10000",
        AdminClientConfig.CONNECTIONS_MAX_IDLE_MS_CONFIG -> "10000",
      ).asJava
    )
  }

  def closeClient(adminClient: AdminClient): IO[Unit] = IO {
    val adminClientCloseTimeout: FiniteDuration = 2.seconds
    adminClient.close(java.time.Duration.ofMillis(adminClientCloseTimeout.toMillis))
  }

  val adminClient = Resource.make(startClient)(closeClient)

}
