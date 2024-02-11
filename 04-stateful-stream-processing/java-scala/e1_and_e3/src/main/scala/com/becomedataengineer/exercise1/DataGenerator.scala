package com.becomedataengineer.exercise1

import org.apache.commons.io.FileUtils
import org.apache.spark.sql.SparkSession

import java.io.File
import java.time.temporal.ChronoUnit
import java.time.{LocalDateTime, ZoneId}
import scala.collection.mutable
import scala.sys.process._
import scala.util.{Random, Try}

object DataGenerator {

  def main(args: Array[String]): Unit = {
    println("Preparing topics")
    Seq(InputTopic, OutputTopic).foreach(topicName => {
      println(s"...${topicName}")
      Try {
        val cmdDelete = s"docker exec e1_kafka kafka-topics.sh --bootstrap-server localhost:29092 --topic ${topicName} --delete"
        val outputDelete = cmdDelete.!!
      }
      val cmdCreate = s"docker exec e1_kafka kafka-topics.sh --bootstrap-server localhost:29092 --topic ${topicName} --create --partitions 2"
      val outputCreate = cmdCreate.!!
    })

    val referenceDataset =
      """{"id": "ff", "name": "Firefox", "version": "1.0"}
        |{"id": "ff", "name": "Firefox", "version": "1.1"}
        |{"id": "ch", "name": "Chrome", "version": "10.0"}
        |{"id": "ch", "name": "Chrome", "version": "10.1"}
        |{"id": "sf", "name": "Safari", "version": "100.0"}
        |""".stripMargin
    FileUtils.writeStringToFile(new File(ReferenceDatasetFileName), referenceDataset, "UTF-8")

    val sparkSession = SparkSession.builder().master("local[*]")
      .getOrCreate()
    import sparkSession.implicits._
    val utc = ZoneId.of("UTC")

    val firstDayToGenerate = LocalDateTime.now(utc).minusDays(10)
    var generationTime = firstDayToGenerate
    val userIds = List(1, 1, 1, 1, 1, 1, 1, 2, 2, 2, 2, 2, 2, 3, 3, 3, 3, 3, 3, 4, 4, 4, 4, 4, 5)
    val browsers = Map(1 -> "ff", 2 -> "ch", 3 -> "sf", 4 -> "ff", 5 -> "ch")
    val browserVersions = Map(1 -> "1.0", 2 -> "10.0", 3 -> "100.0", 4 -> "1.1", 5 -> "10.1")
    val lastTimePerUser = mutable.HashMap(1 -> generationTime, 2 -> generationTime, 3 -> generationTime,
      4 -> generationTime, 5 -> generationTime)
    while (generationTime.isBefore(LocalDateTime.now(utc))) {
      println("Sending records")
      (0 to 10).map(_ => {
        val userId = Random.shuffle(userIds).head
        val (value, unit) = if (Random.nextInt(1000) % 50 == 0) {
          // Session end trigger
          println("Sending session closing event")
          (30L + Random.nextInt(15).toLong, ChronoUnit.MINUTES)
        } else {
          (1L + Random.nextInt(15).toLong, ChronoUnit.SECONDS)
        }
        val eventTime = lastTimePerUser(userId).plus(value, unit)
        lastTimePerUser.put(userId, eventTime)
        val browser = browsers(userId)
        UserVisit(userId, eventTime, browserKey = browser,
          browserVersion = browserVersions(userId))
      })
        .toDF(UserVisit.SchemaAsStructType.fieldNames: _*)
        .selectExpr("TO_JSON(STRUCT(*)) AS value")
        .write.format("kafka")
        .option("topic", InputTopic)
        .option("kafka.bootstrap.servers", "localhost:29092")
        .save()

      generationTime = generationTime.plusMinutes(1L)
      Thread.sleep(5000L)
    }
  }

}