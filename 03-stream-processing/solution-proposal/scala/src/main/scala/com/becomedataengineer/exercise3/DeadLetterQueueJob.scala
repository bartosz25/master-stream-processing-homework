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
        // We call it with a direct `.get`, making the code fail
        // Let's fix it by changing the function and catch any error
        try {
          VisitPageMappingResult(s"""{"page":  "${visit.visitedPage.get}"}""", visit.key, "valid_data")
        } catch {
          case e: Exception => {
            VisitPageMappingResult(visit.asJson, visit.key, "dead_letter")
          }
        }
      })

    val writeQuery = validAndInvalidVisits.select("value", "key", "topic")
      .writeStream.format("kafka")
      .options(
        Map(
          "kafka.bootstrap.servers" -> "localhost:29092",
          "checkpointLocation" -> CheckpointDir
        )
      ).start()

    writeQuery.awaitTermination()
  }

}

case class VisitPageMappingResult(value: String, key: String, topic: String)