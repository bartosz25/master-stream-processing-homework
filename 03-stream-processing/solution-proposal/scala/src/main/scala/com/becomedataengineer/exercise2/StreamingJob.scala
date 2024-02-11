package com.becomedataengineer.exercise2

import com.becomedataengineer.exercise2.SinkConfiguration.SinkOptions
import org.apache.spark.sql.SparkSession

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

    val recordsToWrite = StreamingJobProcessingLogic.processInputData(dataFrame)

    val writeQuery = recordsToWrite.writeStream
      .option("checkpointLocation", "/tmp/bde/module5/homework/e2/checkpoint")
      .format("kafka").options(SinkOptions)

    writeQuery.start().awaitTermination()
  }

}
