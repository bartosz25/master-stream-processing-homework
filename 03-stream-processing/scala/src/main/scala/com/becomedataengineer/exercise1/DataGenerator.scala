package com.becomedataengineer.exercise1

import org.apache.spark.sql.SparkSession

import java.time.{LocalDateTime, ZoneId}
import scala.util.Try
import scala.sys.process._

object DataGenerator {

  def main(args: Array[String]): Unit = {
    println("Preparing topics")
    val topicName = "visits"
    Try {
      val cmdDelete = s"docker exec p1_kafka kafka-topics.sh --bootstrap-server localhost:29092 --topic ${topicName} --delete"
      val outputDelete = cmdDelete.!!
    }
    val cmdCreate = s"docker exec p1_kafka kafka-topics.sh --bootstrap-server localhost:29092 --topic ${topicName} --create --partitions 2"
    val outputCreate = cmdCreate.!!

    val sparkSession = SparkSession.builder().master("local[*]")
      .getOrCreate()
    import sparkSession.implicits._
    val utc = ZoneId.of("UTC")

    val firstDayToGenerate = LocalDateTime.now(utc).minusDays(10)
    var generationTime = firstDayToGenerate
    while (generationTime.isBefore(LocalDateTime.now(utc))) {
      println("Sending records")
      Seq(
        UserVisit().asJson, UserVisit().asJson, UserVisit().asJson, UserVisit().asJson,
        UserVisit().asJson, UserVisit().asJson, UserVisit().asJson, UserVisit().asJson,
        UserVisit().asJson, UserVisit().asJson, UserVisit().asJson, UserVisit().asJson,
        UserVisit().asJson, UserVisit().asJson, UserVisit().asJson, UserVisit().asJson,
        UserVisit().asJson, UserVisit().asJson, UserVisit().asJson, UserVisit().asJson
      ).toDF("value").write.format("kafka")
        .option("topic", topicName)
        .option("kafka.bootstrap.servers", "localhost:29092")
        .save()

      generationTime = generationTime.plusMinutes(1L)
      Thread.sleep(5000L)
    }
  }

}