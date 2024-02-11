package com.becomedataengineer.exercise2.part1

import com.becomedataengineer.VisitTimestampOnlySchema
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession, functions}

import scala.concurrent.duration.DurationInt

object BatchSynchronizer extends App {

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

  val writeQuery = query.writeStream.option("checkpointLocation", s"${prefixDir}/checkpoint")
    .trigger(Trigger.ProcessingTime(3.seconds))
    .foreachBatch((dataset: DataFrame, batchNumber: Long) => {
      val datasetWithValidityFlag = dataset
        .withColumn("is_valid_event_time", functions.when($"data.eventTime".isNull(), false).otherwise(true))

      datasetWithValidityFlag.cache()

      datasetWithValidityFlag.filter("is_valid_event_time == true")
        .withColumn("year", functions.year($"data.eventTime"))
        .withColumn("month", functions.month($"data.eventTime"))
        .withColumn("day", functions.dayofmonth($"data.eventTime"))
        .withColumn("hour", functions.hour($"data.eventTime"))
        .drop("data")
        .write.format("delta").mode(SaveMode.Append)
        .partitionBy("year", "month", "day", "hour").save(s"${dataOutputDir}/valid")

      datasetWithValidityFlag.filter("is_valid_event_time == false")
        .withColumns(
          Map(
            "year" -> functions.year(functions.current_timestamp()),
            "month" -> functions.month(functions.current_timestamp()),
            "day" -> functions.dayofmonth(functions.current_timestamp()),
            "hour" -> functions.hour(functions.current_timestamp()),
          )
        )
        .drop("data")
        .write.format("delta").mode(SaveMode.Append)
        .partitionBy("year", "month", "day", "hour").save(s"${dataOutputDir}/invalid")
  })

  writeQuery.start().awaitTermination()

}