package com.becomedataengineer.exercise2

import org.apache.commons.io.FileUtils
import org.apache.spark.sql.{SparkSession, functions}

import java.io.File
import java.time.format.DateTimeFormatter
import java.time.{LocalDateTime, ZoneId}

import scala.util.Try
import scala.sys.process._

object DataGenerator {

  def main(args: Array[String]): Unit = {
    println("Preparing topics")
    val topicName = "visits_raw"
    Seq(topicName, "valid_visits").foreach(topic => {
      Try {
        val cmdDelete = s"docker exec p2_kafka kafka-topics.sh --bootstrap-server localhost:29092 --topic ${topic} --delete"
        val outputDelete = cmdDelete.!!
      }
      val cmdCreate = s"docker exec p2_kafka kafka-topics.sh --bootstrap-server localhost:29092 --topic ${topic} --create --partitions 2"
      val outputCreate = cmdCreate.!!
    })
    FileUtils.deleteDirectory(new File(FilesOutputDir))

    val sparkSession = SparkSession.builder().master("local[*]")
      .getOrCreate()
    import sparkSession.implicits._
    val utc = ZoneId.of("UTC")

    val firstDayToGenerate = LocalDateTime.now(utc).minusDays(10)
    var generationTime = firstDayToGenerate
    while (generationTime.isBefore(LocalDateTime.now(utc))) {
      println("Generating data")
      val dataToWrite = Seq(
        UserVisit().asJson, UserVisit().asJson, UserVisit().asJson, UserVisit().asJson,
        UserVisit().asJson, UserVisit().asJson, UserVisit().asJson, UserVisit().asJson,
        UserVisit().asJson, UserVisit().asJson, UserVisit().asJson, UserVisit().asJson,
        UserVisit().asJson, UserVisit().asJson, UserVisit().asJson, UserVisit().asJson,
        UserVisit().asJson, UserVisit().asJson, UserVisit().asJson, UserVisit().asJson,
        UserVisit().asJson, UserVisit().asJson, UserVisit().asJson, UserVisit().asJson,
        UserVisit().asJson, UserVisit().asJson, UserVisit().asJson, UserVisit().asJson
      ).toDF("value")

      dataToWrite
        .write.format("kafka")
        .option("topic", topicName)
        .option("kafka.bootstrap.servers", "localhost:29092")
        .save()

      dataToWrite.withColumn("event_time", functions.lit(generationTime.format(DateTimeFormatter.ISO_DATE)))
        .write.format("json")
        .mode("append")
        .partitionBy("event_time")
        .save(FilesOutputDir)

      generationTime = generationTime.plusDays(1L)
      Thread.sleep(500L)
    }
    println("Finished!")
  }

}