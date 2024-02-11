from pyspark.sql import SparkSession, functions, DataFrame
from pyspark.sql.types import StructType, StructField, TimestampType

common_prefix_dir = "/tmp/bde-3/homework/p2/e1"
data_output_dir: str = f"{common_prefix_dir}/output"

spark = SparkSession.builder.master("local[*]") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0,io.delta:delta-core_2.12:2.2.0') \
    .getOrCreate()

input_events = spark.readStream.format('kafka') \
    .option('kafka.bootstrap.servers', 'localhost:29092') \
    .option('subscribe', 'visits') \
    .option('startingOffsets', 'EARLIEST') \
    .load()

visit_timestamp_only_schema = StructType([
    StructField('eventTime', TimestampType())
])

query = input_events.selectExpr('CAST(value AS STRING)') \
    .select(functions.col('value'), functions.from_json('value', visit_timestamp_only_schema).alias('data'))


def write_events_as_delta_lake_records(dataset: DataFrame, batch_number):
    dataset_with_validity_flag = dataset.withColumn('is_valid_event_time',
                                                    functions.when((functions.col("data.eventTime").isNull()),
                                                                   False).otherwise(True))
    dataset_with_validity_flag.cache()

    dataset_with_validity_flag.filter("is_valid_event_time == true") \
        .withColumn("year", functions.year("data.eventTime")) \
        .withColumn("month", functions.month("data.eventTime")) \
        .withColumn("day", functions.dayofmonth("data.eventTime")) \
        .withColumn("hour", functions.hour("data.eventTime")) \
        .drop("data")\
        .write.format("delta").mode('append') \
        .partitionBy("year", "month", "day", "hour").save(f"{data_output_dir}/valid")

    dataset_with_validity_flag.filter("is_valid_event_time == false") \
        .withColumns({
            "year": functions.year(functions.current_timestamp()),
            "month": functions.month(functions.current_timestamp()),
            "day": functions.dayofmonth(functions.current_timestamp()),
            "hour": functions.hour(functions.current_timestamp()),
        }) \
        .drop("data")\
        .write.format("delta").mode('append') \
        .partitionBy("year", "month", "day", "hour").save(f"{data_output_dir}/invalid")


write_query = query.writeStream.option('checkpointLocation', f'{common_prefix_dir}/checkpoint') \
    .trigger(processingTime='20 seconds').foreachBatch(write_events_as_delta_lake_records)

write_query.start().awaitTermination()
