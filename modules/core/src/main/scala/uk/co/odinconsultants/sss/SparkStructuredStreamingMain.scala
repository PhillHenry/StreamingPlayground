package uk.co.odinconsultants.sss
import cats.effect.{Deferred, IO, IOApp}
import cats.free.Free
import com.comcast.ip4s.*
import fs2.kafka.{ConsumerSettings, ProducerRecords, ProducerSettings, *}
import fs2.{Chunk, Pipe, Pure, Stream}
import uk.co.odinconsultants.dreadnought.docker.ZKKafkaMain.waitFor
import uk.co.odinconsultants.dreadnought.docker.{
  CatsDocker,
  Command,
  ContainerId,
  ImageName,
  KafkaAntics,
  LoggingRequest,
  ManagerRequest,
  NamesRequest,
  StartRequest,
  StopRequest,
  ZKKafkaMain,
}
import com.comcast.ip4s.Port
import com.github.dockerjava.api.DockerClient

object SparkStructuredStreamingMain extends IOApp.Simple {

  /** TODO
    * Pull images
    * Timeouts
    */
  def run: IO[Unit] = for {
    client     <- CatsDocker.client
    sparkStart <- Deferred[IO, String]
    spark      <- startMaster(sparkStart, port"8081", port"7077", client)
//    _          <- sparkStart.get
    masterName <- CatsDocker.interpret(client, Free.liftF(NamesRequest(spark)))
    slaveStart <- Deferred[IO, String]
    slave      <- startSlave(slaveStart, port"7077", masterName.head, client)
//    _          <- slaveStart.get
    _          <- CatsDocker.interpret(
                    client,
                    for {
                      _ <- Free.liftF(StopRequest(spark))
                      _ <- Free.liftF(StopRequest(slave))
                    } yield {},
                  )
  } yield println("Started and stopped" + spark)

  def startMaster(
      sparkStart:  Deferred[IO, String],
      webPort:     Port,
      servicePort: Port,
      client:      DockerClient,
  ): IO[ContainerId] = CatsDocker.interpret(client,
    for {
      spark <- Free.liftF(sparkMaster(webPort, servicePort))
      _     <-
        Free.liftF(
          LoggingRequest(spark, waitFor("I have been elected leader! New state: ALIVE", sparkStart))
        )
    } yield spark
  )

  def startSlave(
      sparkStart:  Deferred[IO, String],
      servicePort: Port,
      masterName:  String,
      client:      DockerClient,
  ): IO[ContainerId] = CatsDocker.interpret(client,
    for {
      spark <- Free.liftF(sparkSlave(masterName, servicePort))
      _     <-
        Free.liftF(
          LoggingRequest(spark, waitFor("Successfully started service 'sparkWorker'", sparkStart))
        )
    } yield spark
  )

  def sparkMaster(webPort: Port, servicePort: Port): StartRequest = StartRequest(
    ImageName("bde2020/spark-master:3.2.1-hadoop3.2"),
    Command("/bin/bash /master.sh"),
    List("INIT_DAEMON_STEP=setup_spark"),
    List(8080 -> webPort.value, 7077 -> servicePort.value),
    List.empty,
  )

  def sparkSlave(masterName: String, servicePort: Port): StartRequest = StartRequest(
    ImageName("bde2020/spark-worker:3.2.1-hadoop3.2"),
    Command("/bin/bash /worker.sh"),
    List(s"SPARK_MASTER=spark:/$masterName:${servicePort.value}"),
    List.empty,
    List.empty,
  )

}
