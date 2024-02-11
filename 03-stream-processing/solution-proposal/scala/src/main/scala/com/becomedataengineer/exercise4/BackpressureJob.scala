package com.becomedataengineer.exercise4

import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Dataset, SparkSession, functions}

import java.sql.Timestamp

object DeadLetterQueueJob {

  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().master("local[*]")
      .getOrCreate()
    import sparkSession.implicits._

    val inputStream = sparkSession.readStream.format("kafka")
      .options(Map(
        "kafka.bootstrap.servers" -> "localhost:29092",
        "subscribe" -> "data_for_backpressure",
        "startingOffsets" -> "EARLIEST"
      )).load()


    val rowToProcessSchema = ScalaReflection.schemaFor[RowToProcess].dataType.asInstanceOf[StructType]

    val rowsToProcess = inputStream
      .select(functions.from_json($"value".cast("string"), rowToProcessSchema).as("value"))
      .selectExpr("value.*")
      .as[RowToProcess]

    val writeQuery = rowsToProcess.writeStream
      .foreachBatch((realTimeDataset: Dataset[RowToProcess], batchNumber: Long) => {
        val maybeBackpressuredDataset = if (batchNumber > 0) {
          Some(sparkSession.read.schema(rowToProcessSchema).json(s"${BackpressureDir}/${batchNumber - 1}")
          .as[RowToProcess])
        } else {
          None
        }
        val datasetToProcess = maybeBackpressuredDataset
          .map(backpressuredDataset => backpressuredDataset.union(realTimeDataset))
          .getOrElse(realTimeDataset)

        // Classify the records for the backpressure storage
        val maxRecordsPerJob = 5
        val backpressureWindowSpec = Window.partitionBy("id").orderBy($"generation_time".asc)
        val datasetWithBackpressureFlag = datasetToProcess.withColumn("is_backpressured", functions.when(
          functions.row_number().over(backpressureWindowSpec) > maxRecordsPerJob, true
        ).otherwise(false))

        val cachedDatasetWithBackpressureFlag = datasetWithBackpressureFlag.cache()

        cachedDatasetWithBackpressureFlag.filter("is_backpressured = true")
          .drop("is_backpressured")
          .write.format("json").save(s"${BackpressureDir}/${batchNumber}")

        cachedDatasetWithBackpressureFlag.filter("is_backpressured = false")
          .drop("is_backpressured")
          .show(truncate = false)

        cachedDatasetWithBackpressureFlag.unpersist()
        ()
      })
      .start()

    writeQuery.awaitTermination()
  }

}

case class RowToProcess(id: Int, row_value: String, generation_time: Timestamp)