package uk.co.odinconsultants.kafka

import cats.effect.{Deferred, IO, IOApp}
import cats.free.Free
import com.comcast.ip4s.*
import fs2.Stream
import fs2.kafka.ProducerSettings
import uk.co.odinconsultants.dreadnought.Flow.race
import uk.co.odinconsultants.dreadnought.docker.*
import uk.co.odinconsultants.dreadnought.docker.Algebra.toInterpret
import uk.co.odinconsultants.dreadnought.docker.CatsDocker.interpret
import uk.co.odinconsultants.dreadnought.docker.KafkaAntics.{createCustomTopic, produceMessages}
import uk.co.odinconsultants.dreadnought.docker.Logging.verboseWaitFor
import uk.co.odinconsultants.dreadnought.docker.PopularContainers.startKafkaOnPort
import uk.co.odinconsultants.dreadnought.docker.SparkStructuredStreamingMain.{
  startSlave,
  startSparkCluster,
  waitForMaster,
}
import uk.co.odinconsultants.dreadnought.docker.ZKKafkaMain.{kafkaEcosystem, startKafkaCluster}

import java.util.UUID
import scala.concurrent.duration.*

object KafkaDemoMain extends IOApp.Simple {

  val TOPIC_NAME = "test_topic"
  val BOOTSTRAP  = "kafka_bootstrap"
  val BROKER     = "zk"
  val kafkaPort  = port"9092"
  val clusterId  = UUID.randomUUID().toString

  /** TODO
    * Pull images
    */
  def run: IO[Unit] = for {
    client     <- CatsDocker.client
    kafkaStart <- Deferred[IO, String]
    kafkaLatch  = verboseWaitFor("started (kafka.server.Kafka", kafkaStart)
    kafka2     <- interpret(client, startKafkas(3, kafkaLatch))
    _          <- interpret(
                    client,
                    Free.liftF(
                      LoggingRequest(kafka2, kafkaLatch)
                    ),
                  )
    _          <- kafkaStart.get.timeout(20.seconds)
    _          <- IO(createCustomTopic(TOPIC_NAME))
    _          <- sendMessages
    _          <- race(toInterpret(client))(
                    List(kafka2).map(StopRequest.apply)
                  )
  } yield println("Started and stopped ZK and 2 kafka brokers")

  def startKafkas(numBrokers: Int, kafkaLogging: String => IO[Unit]): Free[ManagerRequest, ContainerId] = {
    val meta   = for {
      brokerId <- 1 to numBrokers
      port     <- Port.fromInt(9091 + brokerId)
    } yield (port, brokerId, s"kafka$brokerId")
    val quorum = meta.map { case (port, brokerId, name) => s"$brokerId@$name:$port" }.mkString(",")
    println(s"quorum = $quorum")
    val frees: Seq[Free[ManagerRequest, ContainerId]] = meta.map { case (port, brokerId, name) =>
      kafkaEcosystem(kafkaLogging, port, brokerId, quorum, name)
    }
    frees.tail.foldLeft(frees.head) { case (x, y) =>
      x.flatMap(_ => y)
    }
  }

  def kafkaEcosystem(
      kafkaLogging: String => IO[Unit],
      hostPort:     Port,
      brokerId:     Int,
      quorum:       String,
      name:         String,
  ): Free[ManagerRequest, ContainerId] =
    for {
      kafka1 <- Free.liftF(startKafkaOnPort(hostPort, brokerId, quorum, name))
      _      <- Free.liftF(
                  LoggingRequest(kafka1, kafkaLogging)
                )
    } yield kafka1

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
