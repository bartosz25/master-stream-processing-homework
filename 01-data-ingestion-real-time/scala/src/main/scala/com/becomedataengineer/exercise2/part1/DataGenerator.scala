package com.becomedataengineer.exercise2.part1

import org.apache.spark.sql.functions.{col, struct}
import org.apache.spark.sql.{SparkSession, functions}

import java.time.{LocalDateTime, ZoneId}
import java.util.UUID

object DataGenerator {

  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().master("local[*]")
      .getOrCreate()
    import sparkSession.implicits._
    val utc = ZoneId.of("UTC")

    val firstDayToGenerate = LocalDateTime.now(utc).minusDays(10)
    var generationTime = firstDayToGenerate
    while (generationTime.isBefore(LocalDateTime.now(utc))) {
      val eventTimesForEvents = (0 to 6).map(_ => {
        if (scala.util.Random.nextInt(30000) % 20 == 0) {
          None
        } else {
          Some(generationTime.toString)
        }
      })

      val visitsToSend = Seq(
        Visit(eventId = UUID.randomUUID().toString, visitId = 1, userId = 1, visitedPage = "/home", eventTime = eventTimesForEvents(0)),
        Visit(eventId = UUID.randomUUID().toString, visitId = 1, userId = 1, visitedPage = "/contact", eventTime = eventTimesForEvents(1)),
        Visit(eventId = UUID.randomUUID().toString, visitId = 1, userId = 1, visitedPage = "/products", eventTime = eventTimesForEvents(2)),
        Visit(eventId = UUID.randomUUID().toString, visitId = 2, userId = 2, visitedPage = "/my-account", eventTime = eventTimesForEvents(3)),
        Visit(eventId = UUID.randomUUID().toString, visitId = 2, userId = 2, visitedPage = "/my-orders", eventTime = eventTimesForEvents(4)),
        Visit(eventId = UUID.randomUUID().toString, visitId = 2, userId = 2, visitedPage = "/my-orders/order/1", eventTime = eventTimesForEvents(5)),
        Visit(eventId = UUID.randomUUID().toString, visitId = 3, userId = 3, visitedPage = "/home", eventTime = eventTimesForEvents(6))
      ).toDF

      visitsToSend.select($"visitId".cast("string").as("key"),
        functions.to_json(struct(visitsToSend.columns.map(col(_)):_*  )).as("value"))
        .write.format("kafka").format("kafka")
        .option("kafka.bootstrap.servers", "localhost:29092")
        .option("topic", "visits")
        .save()

      generationTime = generationTime.plusMinutes(1L)
      Thread.sleep(5000L)
    }
  }

}

case class Visit(eventId: String, visitId: Int, userId: Int, visitedPage: String, eventTime: Option[String])
