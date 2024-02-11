import pyspark.sql.functions as F
from pyspark.sql import SparkSession

from exercise3.mappers import get_visited_page

spark = SparkSession.builder.master("local[*]") \
    .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0') \
    .getOrCreate()

input_data_stream = spark.readStream \
    .option("kafka.bootstrap.servers", "localhost:29092") \
    .option("subscribe", "visits_raw_for_dlq") \
    .option("startingOffsets", "EARLIEST") \
    .format("kafka") \
    .load()


valid_and_invalid_visits = input_data_stream.withColumn("payload", get_visited_page(F.col("value")))


write_query = valid_and_invalid_visits.select("payload.value", "payload.topic", "key")\
    .writeStream.format("kafka")\
    .option("checkpointLocation", "/tmp/bde/module5/homework/e3/checkpoints")\
    .option("kafka.bootstrap.servers", "localhost:29092")\
    .start()

write_query.awaitTermination()
