package com.becomedataengineer

import com.becomedataengineer.dispatcher.model.{CleansedEventLog, EventLog, KafkaSinkParameters, KeyWithValue}
import org.apache.spark.sql.{SparkSession, functions}
object JobExercise4 {

  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().master("local[*]")
      .config("spark.sql.session.timeZone", "UTC")
      .getOrCreate()
    import sparkSession.implicits._

    val deviceReferenceDataset = sparkSession.read.format("jdbc")
      .options(JdbcReferenceDatasetConnectionOptions).load()

    val dataFrame = sparkSession.readStream
      .format("kafka")
      .options(KafkaInputConnectionOptions)
      .load()

    val recordsToWrite = dataFrame
      .select(
        $"key".cast("string"),
        functions.from_json($"value".cast("string"), EventLog.Schema).as("value")
      ).as[KeyWithValue]
      .map(eventLog => eventLog.toCleansedKeyWithValue)
      .withColumn("topic", functions.when(
        ($"value.user_id" === 0 || $"value.visit_id".isNull), "invalid_data"
      ).otherwise("valid_data")
      )
      .withColumn("event_time", $"value.event_time")
      .withColumn("user_id", $"value.user_id")
      .withWatermark("event_time", "10 minutes")
      .dropDuplicates("user_id", "event_time")

    val enrichedRecordsToWrite = recordsToWrite.join(deviceReferenceDataset,
      deviceReferenceDataset("device_type") === recordsToWrite("value.technical.device.type"),
      "leftOuter"
    ).select($"key", $"value", $"topic", $"full_name")
      .as[(String, CleansedEventLog, String, String)]
      .map {
        case (key, value, topic, fullDeviceName) => {
          val oldTechnical = value.technical
          val valueToReturn = oldTechnical.device.map(oldDevice => {
            value.copy(technical = oldTechnical.copy(device = Some(oldDevice.copy(full_name = Some(fullDeviceName)))))
          }).getOrElse(value)
          KafkaSinkParameters(key, valueToReturn, topic)
        }
      }.select($"key", functions.to_json($"value").as("value"), $"topic")

    val writeQuery = enrichedRecordsToWrite.writeStream
      .option("checkpointLocation", s"${CheckpointBaseDir}/job4")
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:29092")
    // As you can see here, I'm not using any output topic configuration because
    // Apache Spark connector is able to "deduce" the topic from the topic column
    // we've created before

    writeQuery.start().awaitTermination()
  }

}
