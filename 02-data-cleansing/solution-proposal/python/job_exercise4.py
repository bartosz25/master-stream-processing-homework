from pyspark.sql import SparkSession, functions
from pyspark.sql.types import StructType, StructField, LongType, TimestampType

from cleansing import mappers
from cleansing.mappers import enrich_output_with_full_device_name
from config import kafka_input_topic, checkpoint_location_for_job

spark = SparkSession.builder.master("local[*]") \
    .config("spark.sql.session.timeZone", "UTC") \
    .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0,org.postgresql:postgresql:42.2.10') \
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
                                           properties={'user': 'bde_user', 'password': 'bde_password',
                                                       'driver': 'org.postgresql.Driver'})

schema_deduplication = StructType([
    StructField("user_id", LongType()),
    StructField("event_time", TimestampType())
])

query = input_data_stream.select(
        functions.col("key").cast("string"),
        functions.from_json(functions.col("value").cast("string"), schema_deduplication).alias("deduplication_columns"),
        functions.col("value").cast("string")) \
    .withColumn("event_time", functions.col("deduplication_columns.event_time")) \
    .withColumn("user_id", functions.col("deduplication_columns.user_id")) \
    .withWatermark("event_time", "10 minutes") \
    .dropDuplicates(["user_id", "event_time"]) \
    .withColumn("cleansed_value", mappers.clean_value(functions.col("value")))\
    .selectExpr("key", "cleansed_value.payload AS value", "cleansed_value.device", "cleansed_value.topic")

enriched_dataset_to_write = query.join(device_reference_dataset,
                                       device_reference_dataset["device_type"] == query["device"],
                                       "left_outer"
                                       )

data_to_write = enriched_dataset_to_write.mapInPandas(enrich_output_with_full_device_name, enriched_dataset_to_write.schema)\
    .select(
    functions.col("key"),
    functions.col("value"),
    functions.col("topic")
)

write_query = data_to_write.writeStream \
    .option("checkpointLocation", checkpoint_location_for_job('job4')) \
    .format('kafka') \
    .option("kafka.bootstrap.servers", "localhost:29092")
# As you can see here, I'm not using any output topic configuration because
# Apache Spark connector is able to "deduce" the topic from the topic column
# we've created before

write_query.start().awaitTermination()
