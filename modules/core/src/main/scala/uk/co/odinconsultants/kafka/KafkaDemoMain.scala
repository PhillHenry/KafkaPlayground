package uk.co.odinconsultants.kafka

import cats.arrow.FunctionK
import cats.effect.{Deferred, IO, IOApp}
import cats.free.Free
import com.comcast.ip4s.*
import com.github.dockerjava.api.DockerClient
import fs2.Stream
import fs2.kafka.{AutoOffsetReset, CommittableConsumerRecord, ConsumerSettings, ProducerSettings}
import uk.co.odinconsultants.dreadnought.Flow.race
import uk.co.odinconsultants.dreadnought.docker.*
import uk.co.odinconsultants.dreadnought.docker.Algebra.toInterpret
import uk.co.odinconsultants.dreadnought.docker.CatsDocker.{client, createNetwork, interpret, interpreter, removeNetwork}
import uk.co.odinconsultants.dreadnought.docker.KafkaAntics.{consume, createCustomTopic, produce, produceMessages}
import uk.co.odinconsultants.dreadnought.docker.Logging.{ioPrintln, verboseWaitFor}
import uk.co.odinconsultants.dreadnought.docker.PopularContainers.startKafkaOnPort
import uk.co.odinconsultants.dreadnought.docker.SparkStructuredStreamingMain.{startSlave, startSparkCluster, waitForMaster}
import uk.co.odinconsultants.dreadnought.docker.ZKKafkaMain.{kafkaEcosystem, startKafkaCluster}
import uk.co.odinconsultants.dreadnought.docker.ContainerId
import fs2.kafka.{ConsumerSettings, ProducerRecords, ProducerSettings, *}
import org.apache.kafka.clients.admin.{AdminClient, AdminClientConfig, NewTopic}
import uk.co.odinconsultants.dreadnought.docker.KafkaRaft
import java.util.{Base64, UUID}
import scala.concurrent.duration.*
import scala.util.Try
import scala.concurrent.duration.*
import org.apache.kafka.clients.admin.NewTopic

import scala.jdk.CollectionConverters.*
import scala.util.Try
import org.apache.kafka.clients.admin.AdminClient

import java.util.concurrent.TimeUnit
import org.apache.kafka.clients.admin.AdminClientConfig

object KafkaDemoMain extends IOApp.Simple {

  val TOPIC_NAME     = "test_topic"
  val kafkaPort      = port"9091"

  val networkName    = "my_network"


  /** TODO
    * Pull images
    */
  def run: IO[Unit] = for {
    client       <- CatsDocker.client
    _            <- removeNetwork(client, networkName).handleErrorWith(x =>
                      IO.println(s"Did not delete network $networkName.\n${x.getMessage}")
                    )
    _            <- createNetwork(client, networkName)
    kafkaStart   <- Deferred[IO, String]
    kafkaLatch    =
      verboseWaitFor(Some(s"${Console.BLUE}kafka1: "))("started (kafka.server.Kafka", kafkaStart)
    loggers       = List(
                      kafkaLatch,
                      ioPrintln(Some(s"${Console.GREEN}kafka2: ")),
                      ioPrintln(Some(s"${Console.YELLOW}kafka3: ")),
                    )
    containers   <-
      interpret(
        client,
        KafkaRaft.startKafkas(loggers, networkName),
      )
    _            <- kafkaStart.get.timeout(20.seconds)
    _            <- IO.println(s"About to create topic $TOPIC_NAME")
    _            <- IO(createCustomTopic(TOPIC_NAME, Port.fromInt(kafkaPort.value + 20).get))
    _            <- IO.println("About to send messages...")
    _            <- sendMessages
    messageLatch <- Deferred[IO, String]
    _            <- IO.println("About to read messages")
    _            <- readMessages(messageLatch)
    _            <- IO.println("Waiting for messages...")
    _            <- messageLatch.get.timeout(20.seconds)
    _            <- IO.println("About to shut down...")
    _            <- race(toInterpret(client))(
                      containers.map(StopRequest.apply)
                    )
  } yield println(s"Started and stopped ${loggers.length} kafka brokers")


  private val sendMessages: IO[Unit] = {
    val bootstrapServer                                        = s"localhost:${kafkaPort.value + 20}"
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

  def readMessages(deferred: Deferred[IO, String]) = {
    val consumerSettings =
      ConsumerSettings[IO, String, String]
        .withAutoOffsetReset(AutoOffsetReset.Earliest)
        .withBootstrapServers(s"localhost:${kafkaPort.value + 21}")
        .withGroupId("group_PH")

    val s = for {
      latch <- Stream.emit(deferred)
      _     <- consume(consumerSettings, TOPIC_NAME).interruptAfter(10.seconds).evalMap {
                 (committable: CommittableConsumerRecord[IO, String, String]) =>
                   latch.complete(committable.record.value) *> IO.println("completed")
               }
    } yield ()

    s.handleErrorWith(x => Stream.eval(IO(x.printStackTrace()))).compile.drain
  }

}
