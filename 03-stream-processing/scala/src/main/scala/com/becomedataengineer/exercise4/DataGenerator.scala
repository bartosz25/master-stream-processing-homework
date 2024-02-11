package com.becomedataengineer.exercise4

import org.apache.spark.sql.{SparkSession, functions}

import java.time.{LocalDateTime, ZoneId}
import scala.util.Try
import scala.sys.process._

object DataGenerator {

  def main(args: Array[String]): Unit = {
    println("Preparing topics")
    val topicName = "data_for_backpressure"
    Try {
      val cmdDelete = s"docker exec p4_kafka kafka-topics.sh --bootstrap-server localhost:29092 --topic ${topicName} --delete"
      val outputDelete = cmdDelete.!!
    }
    val cmdCreate = s"docker exec p4_kafka kafka-topics.sh --bootstrap-server localhost:29092 --topic ${topicName} --create --partitions 2"
    val outputCreate = cmdCreate.!!

    val sparkSession = SparkSession.builder().master("local[*]")
      .getOrCreate()
    import sparkSession.implicits._
    val utc = ZoneId.of("UTC")
    val firstDayToGenerate = LocalDateTime.now(utc).minusDays(10)
    var generationTime = firstDayToGenerate
    while (generationTime.isBefore(LocalDateTime.now(utc))) {
      println("Sending data")
      Seq(
        (1, "A", generationTime.plusSeconds(1)), (1, "E", generationTime.plusSeconds(2)),
        (1, "I", generationTime.plusSeconds(3)), (1, "O", generationTime.plusSeconds(4)),
        (1, "U", generationTime.plusSeconds(4)), (1, "AA", generationTime.plusSeconds(5)),
        (1, "EE", generationTime.plusSeconds(6))
        ).toDF("id", "row_value", "generation_time")
        .withColumn("value", functions.to_json(functions.struct("id", "row_value", "generation_time")))
        .select("value")
        .write.format("kafka")
        .option("topic", topicName)
        .option("kafka.bootstrap.servers", "localhost:29092")
        .save()

      generationTime = generationTime.plusMinutes(1L)
      Thread.sleep(5000L)
    }
  }
}