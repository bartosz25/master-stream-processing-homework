import shutil

from pyspark.sql import SparkSession, DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import StructField, TimestampType, IntegerType, StringType, StructType

backpressure_dir = '/tmp/bde/module5/homework/e4/backpressure'
shutil.rmtree(backpressure_dir, ignore_errors=True)

spark = SparkSession.builder.master("local[*]") \
    .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0') \
    .getOrCreate()

input_data_stream = spark.readStream \
    .option("kafka.bootstrap.servers", "localhost:29092") \
    .option("subscribe", "data_for_backpressure") \
    .option("startingOffsets", "EARLIEST") \
    .format("kafka") \
    .load()

json_schema = StructType([
    StructField("id", IntegerType()), StructField("row_value", StringType()),
    StructField("generation_time", TimestampType())
])

rows_to_process = input_data_stream.withColumn("filtering_struct",
                                                 F.from_json(F.col("value").cast("string"), json_schema))\
    .selectExpr("filtering_struct.*")

max_records_per_job = 5


def print_data_with_backpressure_mechanism(data_frame: DataFrame, batch_version: int) -> None:
    backpressure_dataset = None
    if batch_version > 0:
        backpressure_dataset = spark.read.schema(json_schema).json(f'{backpressure_dir}/{batch_version - 1}')

    dataset_to_process = data_frame if not backpressure_dataset else backpressure_dataset.union(data_frame)

    # Classify the records for the backpressure storage
    backpressure_window_spec = Window.partitionBy("id").orderBy(F.asc("generation_time"))
    dataset_to_process_with_backpressure_flag = dataset_to_process \
        .withColumn("is_backpressured", F.when(
        F.row_number().over(backpressure_window_spec) > max_records_per_job, True).otherwise(False))

    cached_dataset_with_backpressure_flag = dataset_to_process_with_backpressure_flag.cache()

    cached_dataset_with_backpressure_flag.filter("is_backpressured = true").drop("is_backpressured").write.format("json") \
        .mode('overwrite').save(f'{backpressure_dir}/{batch_version}')

    cached_dataset_with_backpressure_flag.filter("is_backpressured = false").drop("is_backpressured").show(truncate=False)

    cached_dataset_with_backpressure_flag.unpersist()
    return None


output_stream = rows_to_process.writeStream.foreachBatch(print_data_with_backpressure_mechanism)

output_stream.start().awaitTermination()
