from pyspark.sql import SparkSession, functions as F, DataFrame

from e3.e3_config import get_input_topic_name, get_output_topic_name

spark_session = SparkSession.builder.master("local[*]")\
    .config("spark.sql.session.timeZone", "UTC")\
    .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0')\
    .config("spark.sql.shuffle.partitions", 2).getOrCreate()


input_data = spark_session.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:29092")\
    .option("subscribe", get_input_topic_name())\
    .option("startingOffsets", "EARLIEST")\
    .load()


windows_with_browser_groups = input_data\
    .select(F.from_json(F.col('value').cast('string'), 'eventTime TIMESTAMP, browser STRING').alias('value'))\
    .selectExpr('value.*')\
    .withWatermark('eventTime', '5 minutes')\
    .groupBy('browser', F.window(F.col("eventTime"), "5 minutes"))

windows_with_browser_aggregation = windows_with_browser_groups.agg(
    F.max('eventTime'), F.count('browser')
)

output_dataframe = windows_with_browser_aggregation.selectExpr('browser', 'window',
                                                               '`count(browser)` AS count',
                                                               'CAST(window.end - `max(eventTime)` AS LONG) AS completeness')

write_query = output_dataframe.selectExpr('TO_JSON(STRUCT(*)) AS value')\
    .writeStream.outputMode('update')\
    .format('kafka')\
    .option('checkpointLocation', '/tmp/bde/6/e3/checkpoint')\
    .option('kafka.bootstrap.servers', 'localhost:29092')\
    .option('topic', get_output_topic_name())

write_query.start().awaitTermination()
