from pyspark.sql import SparkSession, functions
from pyspark.sql.types import StructType, StructField, LongType, StringType, TimestampType

from config import kafka_input_topic, checkpoint_location_for_job

spark = SparkSession.builder.master("local[*]") \
    .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0') \
    .getOrCreate()

input_data_stream = spark.readStream \
    .option("kafka.bootstrap.servers", "localhost:29092") \
    .option("subscribe", kafka_input_topic()) \
    .option("startingOffsets", "EARLIEST") \
    .format("kafka") \
    .load()

# Password shouldn't be stored in plain text! Instead you can use a reference name to the
# values stored in a secrets management store. Here I'm using the plain text only for sake
# of simplicity
device_reference_dataset = spark.read.jdbc(url="jdbc:postgresql:bde", table="bde_schema.devices",
                                           properties={'user': 'bde_user', 'password': 'bde_password'})

schema_data_validation = StructType([
    StructField("user_id", LongType()), StructField("visit_id", StringType()),
    StructField("event_time", TimestampType()), StructField("technical", StructType([
        StructField("device", StructType([
            StructField("type", StringType())
        ]))
    ]))
])

query = input_data_stream.select(
    functions.col("key").cast("string"),
    functions.from_json(functions.col("value").cast("string"), schema_data_validation).alias("validation_data"),
    functions.col("value").cast("string")
).withColumn("topic", functions.when(
    ((functions.col("value.user_id") == 0) | (functions.col("value.visit_id").isNull())), "invalid_data")
             .otherwise("valid_data")) \
    .withColumn("event_time", functions.col("validation_data.event_time")) \
    .withColumn("user_id", functions.col("validation_data.user_id")) \
    .withWatermark("event_time", "10 minutes") \
    .dropDuplicates(["user_id", "event_time"]) \
    .select("key", "value", "topic")

enriched_dataset_to_write = query.join(device_reference_dataset,
                                       device_reference_dataset["device_type"] == query["validation_data.technical.device.type"],
                                       "left_outer").select(
    functions.col("key"), functions.col("topic"),
    functions.to_json(functions.struct("value", "full_name")).alias("value")
)

write_query = enriched_dataset_to_write.writeStream \
    .option("checkpointLocation", checkpoint_location_for_job('job3')) \
    .format('kafka') \
    .option("kafka.bootstrap.servers", "localhost:29092")

# As you can see here, I'm not using any output topic configuration because
# Apache Spark connector is able to "deduce" the topic from the topic column
# we've created before

write_query.start().awaitTermination()
