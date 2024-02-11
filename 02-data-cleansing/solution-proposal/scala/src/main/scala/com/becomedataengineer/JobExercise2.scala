package com.becomedataengineer

import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType, TimestampType}

object JobExercise2 {

  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().master("local[*]")
      .getOrCreate()
    import sparkSession.implicits._

    val dataFrame = sparkSession.readStream
      .format("kafka")
      .options(KafkaInputConnectionOptions)
      .load()

    val schemaForDataValidation = StructType(Array(
      StructField("user_id", LongType), StructField("visit_id", StringType),
      StructField("event_time", TimestampType)
    ))

    val query = dataFrame
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
      .select("key", "value", "topic")

    val writeQuery = query.writeStream
      .option("checkpointLocation", s"${CheckpointBaseDir}/job2")
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:29092")
    // As you can see here, I'm not using any output topic configuration because
    // Apache Spark connector is able to "deduce" the topic from the topic column
    // we've created before

    writeQuery.start().awaitTermination()
  }

}
