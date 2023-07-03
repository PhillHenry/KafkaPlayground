package uk.co.odinconsultants.kafka

import cats.arrow.FunctionK
import cats.effect.{Deferred, IO, IOApp}
import cats.free.Free
import com.comcast.ip4s.*
import com.github.dockerjava.api.DockerClient
import fs2.Stream
import fs2.kafka.ProducerSettings
import uk.co.odinconsultants.dreadnought.Flow.race
import uk.co.odinconsultants.dreadnought.docker.*
import uk.co.odinconsultants.dreadnought.docker.Algebra.toInterpret
import uk.co.odinconsultants.dreadnought.docker.CatsDocker.{client, interpret, interpreter}
import uk.co.odinconsultants.dreadnought.docker.KafkaAntics.{createCustomTopic, produceMessages}
import uk.co.odinconsultants.dreadnought.docker.Logging.{ioPrintln, verboseWaitFor}
import uk.co.odinconsultants.dreadnought.docker.PopularContainers.startKafkaOnPort
import uk.co.odinconsultants.dreadnought.docker.SparkStructuredStreamingMain.{
  startSlave,
  startSparkCluster,
  waitForMaster,
}
import uk.co.odinconsultants.dreadnought.docker.ZKKafkaMain.{kafkaEcosystem, startKafkaCluster}
import uk.co.odinconsultants.dreadnought.docker.ContainerId

import java.util.UUID
import scala.concurrent.duration.*

object KafkaDemoMain extends IOApp.Simple {

  val TOPIC_NAME = "test_topic"
  val kafkaPort  = port"9092"
  val clusterId  = UUID.randomUUID().toString

  /** TODO
    * Pull images
    */
  def run: IO[Unit] = for {
    client     <- CatsDocker.client
    kafkaStart <- Deferred[IO, String]
    kafkaLatch  = verboseWaitFor(Some(Console.BLUE))("started (kafka.server.Kafka", kafkaStart)
    containers <-
      interpret(
        client,
        startKafkas(
          List(kafkaLatch, ioPrintln(Some(Console.GREEN)), ioPrintln(Some(Console.YELLOW)))
        ),
      )
    _          <- kafkaStart.get.timeout(20.seconds)
    _          <- IO(createCustomTopic(TOPIC_NAME))
    _          <- sendMessages
    _          <- race(toInterpret(client))(
                    containers.map(StopRequest.apply)
                  )
  } yield println("Started and stopped ZK and 2 kafka brokers")

  def startKafkas(
      consoleColours: List[String => IO[Unit]]
  ): Free[ManagerRequest, List[ContainerId]] = {
    val meta = for {
      colourBroker <- consoleColours.zipWithIndex
      port         <- Port.fromInt(9091 + colourBroker._2)
    } yield (port, colourBroker._2 + 1, s"kafka${colourBroker._2 + 1}", colourBroker._1)

    val quorum =
      meta.map { case (port, brokerId, name, _) => s"$brokerId@$name:$port" }.mkString(",")

    val frees: Seq[Free[ManagerRequest, ContainerId]] = meta.map {
      case (port, brokerId, name, logger) =>
        for {
          containerId <- Free.liftF(startKafkaOnPort(port, brokerId, quorum, name))
          _           <- Free.liftF(
                           LoggingRequest(containerId, logger)
                         )
        } yield containerId
    }
    frees.tail.foldLeft(frees.head.map(List(_))) { case (x, y) =>
      x.flatMap(ids => y.map(x => ids :+ x))
    }
  }

  def startKafkaOnPort(
      hostPort: Port,
      brokerId: Int,
      quorum:   String,
      name:     String,
  ): StartRequest = StartRequest(
    ImageName("docker.io/bitnami/kafka:3.5"),
    Command("/opt/bitnami/scripts/kafka/entrypoint.sh /run.sh"),
    List(
      "KAFKA_CFG_ZOOKEEPER_CONNECT=zookeeper:2181",
      "ALLOW_PLAINTEXT_LISTENER=yes",
      s"KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://127.0.0.1:${hostPort.value}",
      "KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1",
      "KAFKA_TRANSACTION_ABORT_TIMED_OUT_TRANSACTION_CLEANUP_INTERVAL_MS=60000",
      "KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1",
      "KAFKA_AUTHORIZER_CLASS_NAME=kafka.security.authorizer.AclAuthorizer",
      "KAFKA_ALLOW_EVERYONE_IF_NO_ACL_FOUND=true",
      s"BROKER_ID=$brokerId",
      s"CLUSTER_ID=$clusterId",
      s"KAFKA_CONTROLLER_QUORUM_VOTERS=$quorum",
    ),
    List(9092 -> hostPort.value),
    List.empty,
    Some(name),
  )

  private val sendMessages: IO[Unit] = {
    val bootstrapServer                                        = s"localhost:${kafkaPort.value}"
    val producerSettings: ProducerSettings[IO, String, String] =
      ProducerSettings[IO, String, String]
        .withBootstrapServers(bootstrapServer)
    val messages                                               = KafkaAntics
      .produce(producerSettings, TOPIC_NAME)
      .handleErrorWith(x => Stream.eval(IO(x.printStackTrace())))
      .compile
      .drain
    messages
  }

}
