package com.becomedataengineer.exercise3

import org.apache.spark.sql.SparkSession

import java.time.{LocalDateTime, ZoneId}
import scala.sys.process._
import scala.util.{Random, Try}

object DataGenerator {

  def main(args: Array[String]): Unit = {
    val topicsToCreate = Seq(InputTopic, OutputTopic)
    println(s"Preparing topics ${topicsToCreate}")
    topicsToCreate.foreach(topic => {
      Try {
        val cmdDelete = s"docker exec e3_kafka kafka-topics.sh --bootstrap-server localhost:29092 --topic ${topic} --delete"
        val outputDelete = cmdDelete.!!
      }
      val cmdCreate = s"docker exec e3_kafka kafka-topics.sh --bootstrap-server localhost:29092 --topic ${topic} --create --partitions 2"
      val outputCreate = cmdCreate.!!
    })


    val sparkSession = SparkSession.builder().master("local[*]")
      .getOrCreate()
    import sparkSession.implicits._
    val utc = ZoneId.of("UTC")

    val firstDayToGenerate = LocalDateTime.now(utc).minusDays(10)
    var generationTime = firstDayToGenerate
    val userIds = List(1, 1, 1, 1, 1, 1, 1, 2, 2, 2, 2, 2, 2, 3, 3, 3, 3, 3, 3, 4, 4, 4, 4, 4, 5)
    while (generationTime.isBefore(LocalDateTime.now(utc))) {
      println("Sending records")
      (0 to 10).map(_ => {
        val eventTime = generationTime.plusSeconds(Random.nextInt(120).toLong)
        UserVisit(Random.shuffle(userIds).head, eventTime.toString).asJson
      })
        .toDF("value").write.format("kafka")
        .option("topic", InputTopic)
        .option("kafka.bootstrap.servers", "localhost:29092")
        .save()

      generationTime = generationTime.plusMinutes(1L)
      Thread.sleep(2000L)
    }
  }

}