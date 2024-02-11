from datetime import datetime

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StructType, StructField, IntegerType, LongType, TimestampType, ArrayType, \
    StringType

from e1.e1_config import get_input_topic_name, get_output_topic_name, get_reference_dataset_location
from e1.visits_mapper import generate_visit_output

spark_session = SparkSession.builder.master("local[*]") \
    .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0') \
    .config("spark.sql.shuffle.partitions", 2).getOrCreate()

input_data = spark_session.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:29092") \
    .option("subscribe", get_input_topic_name()) \
    .option("startingOffsets", "EARLIEST") \
    .load()

input_visits = input_data.selectExpr('CAST(value AS STRING)', 'timestamp') \
    .select(F.from_json(F.col('value'), "userId INT, eventTime TIMESTAMP, browserKey STRING,"
                                        "browserVersion STRING, visitedPage STRING")
            .alias('data'), 'timestamp') \
    .selectExpr('data.*', 'timestamp')

valid_visits = input_visits.filter('visitedPage IS NOT NULL AND eventTime IS NOT NULL')
user_visits = valid_visits.withWatermark('eventTime', '15 minutes') \
    .groupBy(F.col("userId"))

user_sessions = user_visits.applyInPandasWithState(
    func=generate_visit_output,
    outputStructType=StructType([
        StructField("sessionId", StringType()),
        StructField("userId", IntegerType()),
        StructField("startTime", TimestampType()),
        StructField("endTime", TimestampType()),
        StructField("browserCode", StringType()),
        StructField("browserVersion", StringType()),
        StructField("navigation", ArrayType(StructType([
            StructField("page", StringType()), StructField("timeSpent", LongType())
        ])))
    ]),
    stateStructType=StructType([
        StructField("firstEventTimeEpochMillis", LongType()),
        StructField("pages", ArrayType(StructType([
            StructField("visitedPage", StringType()), StructField("eventTimeAsMilliseconds", LongType())
        ]))),
        StructField("browserCode", StringType()),
        StructField("browserVersion", StringType())
    ]),
    outputMode="update",
    timeoutConf="EventTimeTimeout"
)

technical_reference_dataset = spark_session.read.schema('id STRING, name STRING, version STRING') \
    .json(get_reference_dataset_location())

session_to_write = user_sessions.join(technical_reference_dataset,
                                      F.expr('id = browserCode AND version = browserVersion'), 'left_outer') \
    .withColumn('technicalContext', F.struct('name', 'version')) \
    .drop('browserCode', 'browserVersion', 'id', 'name', 'version') \
    .selectExpr('TO_JSON(STRUCT(*)) AS value')

write_query = session_to_write.writeStream.outputMode("update") \
    .option('checkpointLocation', "/tmp/bde/6/e1/checkpoint/stateful/") \
    .format('kafka')\
    .option('kafka.bootstrap.servers', 'localhost:29092') \
    .option('topic', get_output_topic_name()) \
    .start()

write_query.awaitTermination()
