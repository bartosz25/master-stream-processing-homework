package com.becomedataengineer.exercise1

import org.apache.spark.sql.streaming.{GroupStateTimeout, OutputMode}
import org.apache.spark.sql.{Dataset, KeyValueGroupedDataset, SparkSession, functions}

import java.util.TimeZone
import java.util.concurrent.TimeUnit

object StatefulPipeline {

  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().master("local[*]")
      .config("spark.sql.shuffle.partitions", 2)
      .config("spark.sql.session.timeZone", "UTC")
      .getOrCreate()
    TimeZone.setDefault(TimeZone.getTimeZone("UTC"))
    import sparkSession.implicits._

    val inputStream = sparkSession.readStream.format("kafka")
      .options(Map(
        "kafka.bootstrap.servers" -> "localhost:29092",
        "subscribe" -> InputTopic,
        "startingOffsets" -> "EARLIEST"
      )).load()

    val inputVisits = inputStream.selectExpr("CAST(value AS STRING) AS value", "timestamp")
      .select(functions.from_json($"value".cast("string"), UserVisitWithTimestamp.SchemaAsStructType).as("visit"),
        $"timestamp")
      .selectExpr("visit.*", "timestamp")
      .as[UserVisitWithTimestamp]

    val validVisits = inputVisits.filter(visit => {
      visit.visitedPage != null && visit.eventTime != null
    })
    val sessionTimeout = TimeUnit.MINUTES.toMillis(16)
    val userVisits: KeyValueGroupedDataset[Int, UserVisitWithTimestamp] = validVisits
      .withWatermark("eventTime", "15 minutes")
      .groupByKey(_.userId)
    val userSessions: Dataset[Option[VisitOutput]] = userVisits.mapGroupsWithState(
      GroupStateTimeout.EventTimeTimeout()
    )(
      VisitsMapper.generateVisitDuration(sessionTimeout)
    )

    val technicalReferenceDataset = sparkSession.read
      .schema("id STRING, name STRING, version STRING")
      .json(ReferenceDatasetFileName)

    val sessionsToWrite = userSessions.filter(session => session.isDefined)
      .map(session => session.get)
      .join(technicalReferenceDataset,
        functions.expr("""
          |id = browserCode AND version = browserVersion
          |""".stripMargin), "left_outer")
      .withColumn("technicalContext", functions.struct($"name", $"version"))
      .drop("browserCode", "browserVersion", "id", "name", "version")
      .selectExpr("TO_JSON(STRUCT(*)) AS value")

    val writeQuery = sessionsToWrite.writeStream
      .outputMode(OutputMode.Update())
      .format("kafka")
      .options(
        Map(
          "checkpointLocation" -> "/tmp/bde/6/e1/checkpoint/stateful",
          "kafka.bootstrap.servers" -> "localhost:29092",
          "topic" -> OutputTopic
        )
      )
      .start()

    writeQuery.awaitTermination()
  }

}