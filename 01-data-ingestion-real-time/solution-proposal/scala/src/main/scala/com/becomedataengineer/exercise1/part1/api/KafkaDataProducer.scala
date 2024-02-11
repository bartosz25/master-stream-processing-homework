package com.becomedataengineer.exercise1.part1.api

import org.apache.commons.io.FileUtils
import org.apache.kafka.clients.producer.{Callback, KafkaProducer, ProducerRecord, RecordMetadata}

import java.io.File
import java.util.Properties
import java.util.concurrent.TimeUnit
import scala.collection.mutable

class KafkaDataProducer {

  // We assume here the simplicity; on production we could have a pool of available producers matching the
  // concurrency level of our app to avoid recreating them each time, or a single instance with a better
  // batching configuration
  private val KafkaProducerInstance = {
    val producerProperties = new Properties;
    producerProperties.setProperty("bootstrap.servers", "localhost:29092");
    producerProperties.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    producerProperties.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    producerProperties.setProperty("linger.ms", TimeUnit.SECONDS.toMillis(1L).toString)
    producerProperties.setProperty("batch.size", "12KB")
    new KafkaProducer[String, String](producerProperties)
  }

  private val OutputTopic = "raw_data"

  private val kafkaProducerCallbacksHolder = new KafkaProducerCallbacksHolder()

  def produceDataToKafka(visitsToIngest: VisitsToIngest) = {
    visitsToIngest.visits.foreach(visit => {
      val eventJson = JsonMapper.Instance.writeValueAsString(visit)
      KafkaProducerInstance.send(new ProducerRecord[String, String](
        OutputTopic, visit.visitId.toString, eventJson
      ), kafkaProducerCallbacksHolder.registerCallbackForMessage(eventJson))
    })

    KafkaProducerInstance.flush()
  }

  def saveUndeliveredEvents() = {
    if (kafkaProducerCallbacksHolder.eventsToDeliver.nonEmpty) {
      FileUtils.writeStringToFile(new File(s"/tmp/bde-3/homework/p1/e1/fallback_${System.currentTimeMillis()}.json"),
        kafkaProducerCallbacksHolder.eventsToDeliver.mkString("\n"), "UTF-8")
    }
  }

}

class KafkaProducerCallbacksHolder {

  val eventsToDeliver = new mutable.HashSet[String]()

  class KafkaProducerCallback(event: String) extends Callback {

    // We keep the events management in a single place, only within this class
    eventsToDeliver.add(event)

    override def onCompletion(metadata: RecordMetadata, exception: Exception): Unit = {
      if (exception == null) {
        eventsToDeliver.remove(event)
      } else {
        exception.printStackTrace()
      }
    }
  }

  def registerCallbackForMessage(event: String): Callback = {
    new KafkaProducerCallback(event)
  }

}