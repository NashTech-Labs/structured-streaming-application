package knolx.kafka

import java.util.Properties

import akka.actor.ActorSystem
import knolx.Config.{bootstrapServer, topic}
import knolx.KnolXLogger
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.DurationInt
import scala.language.postfixOps
import scala.util.Random

/**
  * Copyright Knoldus Inc.. All rights reserved.
  */
object MultiDataStreamer extends App with KnolXLogger {
  val system = ActorSystem("DataStreamer")

  val props = new Properties()
  props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer)
  props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
  props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)

  val producer = new KafkaProducer[String, String](props)

  info("Streaming data into Kafka...")
  system.scheduler.schedule(0 seconds, 3000 milliseconds) {
    (1 to Random.nextInt(100)).foreach { id =>
      producer.send(new ProducerRecord[String, String](topic,s"device$id", (Math.random * 2 + 1).toString))
    }
  }
}
