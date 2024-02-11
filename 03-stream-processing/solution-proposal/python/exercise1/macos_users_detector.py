from pyspark.sql import SparkSession, functions as F, functions
from pyspark.sql.types import StructType, StructField, StringType

spark = SparkSession.builder.master("local[*]") \
    .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0') \
    .getOrCreate()

input_data_stream = spark.readStream \
    .option("kafka.bootstrap.servers", "localhost:29092") \
    .option("subscribe", "visits") \
    .option("startingOffsets", "EARLIEST") \
    .format("kafka") \
    .load()

schema_for_filtering = StructType([
    StructField("eventId", StringType()),
    StructField("os", StringType()),
    StructField("visitedPage", StringType())
])

only_macos_events = input_data_stream.withColumn("filtering_struct",
                                                 F.from_json(F.col("value").cast("string"), schema_for_filtering))\
    .filter("filtering_struct.os = 'macOS'")\
    .selectExpr("filtering_struct.*")\
    .withColumn("processedAt", functions.current_timestamp())


write_query = only_macos_events\
    .writeStream.format("json").options(
        path="/tmp/bde/module5/homework/e1/output",
        checkpointLocation="/tmp/bde/module5/homework/e1/checkpoints")\
    .start()

write_query.awaitTermination()
