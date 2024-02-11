package com.becomedataengineer.exercise1

import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

object MacOsUsersDetector {

  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().master("local[*]")
      .getOrCreate()
    import sparkSession.implicits._

    val inputStream = sparkSession.readStream.format("kafka")
      .options(Map(
        "kafka.bootstrap.servers" -> "localhost:29092",
        "subscribe" -> "visits",
        "startingOffsets" -> "EARLIEST"
      )).load()

    val schemaForFiltering = StructType(Array(
      StructField("eventId", StringType),
      StructField("os", StringType),
      StructField("visitedPage", StringType)
    ))

    val onlyMacOsEvents = inputStream
      .withColumn("filtering_struct", functions.from_json($"value".cast("string"), schemaForFiltering))
      .filter("filtering_struct.os = 'macOS'")
      .selectExpr("filtering_struct.*")
      .withColumn("processedAt", functions.current_timestamp())

    val writeQuery = onlyMacOsEvents
      .writeStream.format("json")
      .options(Map(
        "path" -> MacOsEventsOutputDir,
        "checkpointLocation" -> CheckpointDir
      )).start()

    writeQuery.awaitTermination()
  }

}