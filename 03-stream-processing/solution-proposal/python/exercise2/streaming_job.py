from pyspark.sql import SparkSession

from exercise2.configuration import get_sink_configuration
from exercise2.streaming_job_processing_logic import process_input_data

spark = SparkSession.builder.master("local[*]") \
    .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0') \
    .getOrCreate()

input_data_stream = spark.readStream \
    .option("kafka.bootstrap.servers", "localhost:29092") \
    .option("subscribe", "visits_raw") \
    .option("startingOffsets", "EARLIEST") \
    .format("kafka") \
    .load()


records_to_write = process_input_data(input_data_stream)

write_query = records_to_write.writeStream.format("kafka")\
    .option("checkpointLocation", "/tmp/bde/module5/homework/e2/checkpoint")\
    .options(**get_sink_configuration())\
    .start()

write_query.awaitTermination()
