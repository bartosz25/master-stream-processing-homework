from pyspark.sql import SparkSession, functions
from pyspark.sql.types import StructType, StructField, LongType, StringType

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

schema_data_validation = StructType([
    StructField("user_id", LongType()), StructField("visit_id", StringType())
])

query = input_data_stream.select(
            functions.col("key").cast("string"),
            functions.from_json(functions.col("value").cast("string"), schema_data_validation).alias("validation_data"),
            functions.col("value").cast("string")
        ).withColumn("topic", functions.when(
            ((functions.col("value.user_id") == 0) | (functions.col("value.visit_id").isNull())), "invalid_data")
                        .otherwise("valid_data"))\
            .select("key", "value", "topic")

write_query = query.writeStream\
    .option("checkpointLocation", checkpoint_location_for_job('job1'))\
    .format('kafka')\
    .option("kafka.bootstrap.servers", "localhost:29092")

# As you can see here, I'm not using any output topic configuration because
# Apache Spark connector is able to "deduce" the topic from the topic column
# we've created before

write_query.start().awaitTermination()
