package com.becomedataengineer.exercise2.part1

import com.becomedataengineer.VisitTimestampOnlySchema
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.{SparkSession, functions}

import scala.concurrent.duration.DurationInt

object BatchSynchronizerStreamingQueries extends App {

  val prefixDir = "/tmp/bde-3/homework/p2/e1"
  val dataOutputDir = s"${prefixDir}/output"

  val sparkSession = SparkSession.builder()
    .master("local[*]")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
  import sparkSession.implicits._

  val dataFrame = sparkSession.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:29092")
    .option("subscribe", "visits")
    .option("startingOffsets", "EARLIEST")
    .load()

  val query = dataFrame.selectExpr("CAST(value AS STRING)")
    .select($"value", functions.from_json($"value", VisitTimestampOnlySchema).as("data"))
    .withColumn("is_valid_event_time", functions.when($"data.event_time".isNull, false).otherwise(true))

  val writeQueryValid = query.filter("is_valid_event_time == true")
    .withColumn("year", functions.year($"data.event_time"))
    .withColumn("month", functions.month($"data.event_time"))
    .withColumn("day", functions.dayofmonth($"data.event_time"))
    .withColumn("hour", functions.hour($"data.event_time"))
    .writeStream.option("checkpointLocation", s"${prefixDir}/checkpoint_valid")
    .trigger(Trigger.ProcessingTime(3.seconds))
    .format("delta")
    .option("path", s"${dataOutputDir}/valid")
    .partitionBy("year", "month", "day", "hour")
    .start()

  val writeQueryInvalid = query.filter("is_valid_event_time == false")
    .withColumns(
      Map(
        "year" -> functions.year(functions.current_timestamp()),
        "month" -> functions.month(functions.current_timestamp()),
        "day" -> functions.dayofmonth(functions.current_timestamp()),
        "hour" -> functions.hour(functions.current_timestamp()),
      )
    )
    .writeStream.option("checkpointLocation", s"${prefixDir}/checkpoint_invalid")
    .trigger(Trigger.ProcessingTime(3.seconds))
    .format("delta")
    .option("path", s"${dataOutputDir}/invalid")
    .partitionBy("year", "month", "day", "hour")
    .start()

  sparkSession.streams.awaitAnyTermination()

}