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

import scala.concurrent.duration.*

object KafkaDemoMain extends IOApp.Simple {

  val TOPIC_NAME = "test_topic"
  val BOOTSTRAP  = "kafka_bootstrap"
  val BROKER     = "zk"
  val kafkaPort  = port"9092"

  /** TODO
    * Pull images
    */
  def run: IO[Unit] = for {
    client     <- CatsDocker.client
    kafkaStart <- Deferred[IO, String]
    kafkaLatch  = verboseWaitFor("started (kafka.server.Kafka", kafkaStart)
    kafka2     <- interpret(client, kafkaEcosystem(kafkaLatch, kafkaPort))
    _          <- interpret(
                    client,
                    Free.liftF(
                      LoggingRequest(kafka2, kafkaLatch)
                    ),
                  )
    _          <- kafkaStart.get.timeout(20.seconds)
//    _            <- sendMessages
    _          <- race(toInterpret(client))(
                    List(kafka2).map(StopRequest.apply)
                  )
  } yield println("Started and stopped ZK and 2 kafka brokers")

  def kafkaEcosystem(
      kafkaLogging: String => IO[Unit],
      hostPort:     Port,
  ): Free[ManagerRequest, ContainerId] =
    for {
      kafka1 <- Free.liftF(startKafkaOnPort(hostPort))
      _      <- Free.liftF(
                  LoggingRequest(kafka1, kafkaLogging)
                )
    } yield kafka1

  def startKafkaOnPort(
      hostPort: Port
  ): StartRequest = StartRequest(
    ImageName("docker.io/bitnami/kafka:3.5"),
    Command("/opt/bitnami/scripts/kafka/entrypoint.sh /run.sh"),
    List(
      "KAFKA_CFG_ZOOKEEPER_CONNECT=zookeeper:2181",
      "ALLOW_PLAINTEXT_LISTENER=yes",
      //      "KAFKA_INTER_BROKER_LISTENER_NAME=LISTENER_BOB",
      //      "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=LISTENER_BOB:PLAINTEXT",
      s"KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://127.0.0.1:${hostPort.value}",
      "KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1",
      "KAFKA_TRANSACTION_ABORT_TIMED_OUT_TRANSACTION_CLEANUP_INTERVAL_MS=60000",
      "KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1",
      "KAFKA_AUTHORIZER_CLASS_NAME=kafka.security.authorizer.AclAuthorizer",
      "KAFKA_ALLOW_EVERYONE_IF_NO_ACL_FOUND=true",
    ),
    List(9092 -> hostPort.value),
    List.empty,
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
    IO(createCustomTopic(TOPIC_NAME)) *> messages
  }

}
