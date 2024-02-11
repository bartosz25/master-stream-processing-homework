package com.becomedataengineer.exercise2

import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{SparkSession, functions}

object StreamingJob {

  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().master("local[*]")
      .getOrCreate()
    import sparkSession.implicits._

    val dataFrame = sparkSession.readStream
      .format("kafka")
      .options(Map(
        "kafka.bootstrap.servers" -> "localhost:29092",
        "subscribe" -> "visits_raw",
        "startingOffsets" -> "EARLIEST"
      ))
      .load()

    val schemaForFiltering = StructType(Array(
      StructField("os", StringType),
      StructField("visitedPage", StringType)
    ))

    val recordsToWrite = dataFrame
      .select(
        $"key".cast("string"),
        functions.from_json($"value".cast("string"), schemaForFiltering).as("visit"),
        $"value".cast("string")
      )
      .filter("visit.os IN ('macOS', 'Windows', 'Linux')")
      .filter("visit.visitedPage != ''")
      .select($"key", $"value")

    val writeQuery = recordsToWrite.writeStream
      .option("checkpointLocation", "/tmp/bde/module5/homework/e2/checkpoint")
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:29092")
      .option("topic", "valid_visits")

    writeQuery.start().awaitTermination()
  }

}
