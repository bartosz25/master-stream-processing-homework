package com.becomedataengineer.exercise1

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession, functions}

import java.time.LocalDateTime

object StatelessPipeline {

  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().master("local[*]")
      .getOrCreate()
    import sparkSession.implicits._

    val inputStream = sparkSession.readStream.format("kafka")
      .options(Map(
        "kafka.bootstrap.servers" -> "localhost:29092",
        "subscribe" -> InputTopic,
        "startingOffsets" -> "EARLIEST"
      )).load()

    val userVisits = inputStream.selectExpr("CAST(value AS STRING) AS value")
      .select(functions.from_json($"value".cast("string"), UserVisit.SchemaAsStructType).as("visit"))
      .selectExpr("visit.*")
      .as[UserVisit]

    val visitsMappedToOutputFormat = userVisits.filter(visit => {
      visit.visitedPage != null && visit.eventTime != null
    }).withColumnRenamed("visitedPage", "page")

    val writeQuery = visitsMappedToOutputFormat.writeStream
      .option("checkpointLocation", s"/tmp/bde/6/e1/checkpoint/${System.currentTimeMillis()}")
      .foreachBatch((dataset: DataFrame, batchNumber: Long) => {
        dataset.write.mode(SaveMode.Append).format("jdbc")
          .option("driver", "org.postgresql.Driver")
          .option("url", "jdbc:postgresql:bde_h6")
          .option("dbtable", "bde_h6.visits")
          .option("user", "bde_user")
          .option("password", "bde_password")
          .save()
        ()
      })
      .start()

    writeQuery.awaitTermination()
  }

}

case class OutputForStatelessDatabase(userId: Int, eventTime: LocalDateTime,
                                      browserKey: String,
                                      browserVersion: String,
                                      page: String)