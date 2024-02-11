package com.becomedataengineer.exercise3

import org.apache.spark.sql.{SparkSession, functions}

import java.util.TimeZone

object VisitsCounterPerBrowserJob {

  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().master("local[*]")
      .config("spark.sql.session.timeZone", "UTC")
      .getOrCreate()
    TimeZone.setDefault(TimeZone.getTimeZone("UTC"))
    import sparkSession.implicits._

    val inputStream = sparkSession.readStream.format("kafka")
      .options(Map(
        "kafka.bootstrap.servers" -> "localhost:29092",
        "subscribe" -> InputTopic,
        "startingOffsets" -> "EARLIEST",
      )).load()

    val windowsWithBrowserCount = inputStream
      .select(functions.from_json(
        $"value".cast("string"), "eventTime TIMESTAMP, browser STRING", Map.empty[String, String])
        .as("value"))
      .selectExpr("value.*")
      .withWatermark("eventTime", "5 minutes")
      .groupBy($"browser", functions.window($"eventTime", "5 minutes"))
      .agg("eventTime" -> "max", "browser" -> "count")
      .selectExpr("browser", "window",
        "`count(browser)` AS count",
        "CAST(window.end - `max(eventTime)` AS LONG) AS completeness")

    val writeQuery = windowsWithBrowserCount
      .selectExpr("TO_JSON(STRUCT(*)) AS value")
      .writeStream.outputMode("update")
      .format("kafka")
      .options(Map(
        "checkpointLocation" -> "/tmp/bde/6/e3/checkpoint",
        "kafka.bootstrap.servers" -> "localhost:29092",
        "topic"-> OutputTopic
      )).start()

    writeQuery.awaitTermination()
  }

}
