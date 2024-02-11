package com.becomedataengineer.exercise3

import org.apache.spark.sql.{SparkSession, functions}

object DeadLetterQueueJob {

  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().master("local[*]")
      .getOrCreate()
    import sparkSession.implicits._

    val inputStream = sparkSession.readStream.format("kafka")
      .options(Map(
        "kafka.bootstrap.servers" -> "localhost:29092",
        "subscribe" -> "visits_raw_for_dlq",
        "startingOffsets" -> "EARLIEST"
      )).load()

    val validAndInvalidVisits = inputStream
      .select(functions.from_json($"value".cast("string"), UserVisit.Schema).as("value"))
      .selectExpr("value.*", "key")
      .as[UserVisit]
      .map(visit => {
        // Whoops, the visitedPage is an `Option[String]`, so it can be missing!
        // But we call it with a direct `.get`, making the code fail

        // Please fix the issue so that the job doesn't fail and the invalid records goes to the dead_letter topic
        VisitPageMappingResult(s"""{"page":  "${visit.visitedPage.get}"}""", visit.key)
      })

    val writeQuery = validAndInvalidVisits.select("key", "value")
      .writeStream.format("kafka")
      .options(
        Map(
          "kafka.bootstrap.servers" -> "localhost:29092",
          "topic" -> "valid_data",
          "checkpointLocation" -> CheckpointDir
        )
      ).start()

    writeQuery.awaitTermination()
  }

}

case class VisitPageMappingResult(value: String, key: String)