from pyspark.sql import SparkSession

from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StringType, StructField

spark = SparkSession.builder.master("local[*]") \
    .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0') \
    .getOrCreate()

input_data_stream = spark.readStream \
    .option("kafka.bootstrap.servers", "localhost:29092") \
    .option("subscribe", "visits_raw") \
    .option("startingOffsets", "EARLIEST") \
    .format("kafka") \
    .load()

user_visit_schema = StructType([
    StructField("os", StringType()),
    StructField("visitedPage", StringType())
])

records_to_write = input_data_stream.select(
        F.col("key").cast("string"),
        F.from_json(F.col("value").cast("string"), user_visit_schema).alias("visit"),
        F.col("value").cast("string")
    ).filter("visit.os IN ('macOS', 'Windows', 'Linux')").filter("visit.visitedPage != ''") \
        .select(
        F.col("key"), F.col("value")
    )

write_query = records_to_write.writeStream.format("kafka")\
    .option("kafka.bootstrap.servers", "localhost:29092").option("topic", "valid_visits")\
    .start()

write_query.awaitTermination()
