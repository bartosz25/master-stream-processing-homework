package com.becomedataengineer

import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType, TimestampType}

object JobExercise3 {

  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().master("local[*]")
      .getOrCreate()
    import sparkSession.implicits._

    val deviceReferenceDataset = sparkSession.read.format("jdbc")
      .options(JdbcReferenceDatasetConnectionOptions).load()

    val dataFrame = sparkSession.readStream
      .format("kafka")
      .options(KafkaInputConnectionOptions)
      .load()

    val schemaForDataValidation = StructType(Array(
      StructField("user_id", LongType), StructField("visit_id", StringType),
      StructField("event_time", TimestampType), StructField("technical", StructType(Array(
        StructField("device", StructType(Array(
          StructField("type", StringType)
        )))
      )))
    ))

    val recordsToWrite = dataFrame
      .select(
        $"key".cast("string"),
        functions.from_json($"value".cast("string"), schemaForDataValidation).as("validation_data"),
        $"value".cast("string")
      )
      .withColumn("topic", functions.when(
        ($"validation_data.user_id" === 0 || $"validation_data.visit_id".isNull), "invalid_data"
      ).otherwise("valid_data")
      )
      .withColumn("event_time", $"validation_data.event_time")
      .withColumn("user_id", $"validation_data.user_id")
      .withWatermark("event_time", "10 minutes")
      .dropDuplicates("user_id", "event_time")

    val enrichedRecordsToWrite = recordsToWrite.join(deviceReferenceDataset,
      deviceReferenceDataset("device_type") === recordsToWrite("validation_data.technical.device.type"),
      "leftOuter"
    ).select(
      $"key", $"topic",
      functions.to_json(functions.struct($"value", $"full_name")).as("value")
    )

    val writeQuery = enrichedRecordsToWrite.writeStream
      .option("checkpointLocation", s"${CheckpointBaseDir}/job3")
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:29092")
    // As you can see here, I'm not using any output topic configuration because
    // Apache Spark connector is able to "deduce" the topic from the topic column
    // we've created before

    writeQuery.start().awaitTermination()
  }


}
