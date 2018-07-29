package knolx.kafka

import java.util.Properties

import akka.actor.ActorSystem
import knolx.Config.{bootstrapServer, topic}
import knolx.KnolXLogger
import knolx.spark.Stock
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer
import org.json4s.NoTypeHints
import org.json4s.jackson.Serialization
import org.json4s.jackson.Serialization.write

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.DurationInt
import scala.util.Random

/**
  * Copyright Knoldus Inc.. All rights reserved.
  */
object StreamStaticDataGenerator extends App with KnolXLogger {
  val system = ActorSystem("DataStreamer")
  val props = new Properties()
  props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer)
  props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
  props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)

  val producer = new KafkaProducer[String, String](props)

  val companyNames = List("kirloskar", "bajaj", "amul", "dlf", "ebay")
  val orderTypes = List("buy", "sell")
  val numberOfSharesList = List(1, 2, 3, 4, 5, 6, 7, 8, 9)

  implicit val formats = Serialization.formats(NoTypeHints)
  info("Streaming data into Kafka...")
  system.scheduler.schedule(0 seconds, 5 seconds) {
    Random.shuffle(companyNames).headOption.foreach { name =>
      val stock = Stock(name, Random.shuffle(numberOfSharesList).head, Random.shuffle(orderTypes).head)
      producer.send(new ProducerRecord[String, String](topic, write(stock)))
    }
  }
}
