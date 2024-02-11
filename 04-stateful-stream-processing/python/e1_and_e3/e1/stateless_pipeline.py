from datetime import datetime

from pyspark.pandas import DataFrame
from pyspark.sql import SparkSession, functions as F

from e1.e1_config import get_input_topic_name, get_output_topic_name

spark_session = SparkSession.builder.master("local[*]") \
    .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0,org.postgresql:postgresql:42.5.0') \
    .config("spark.sql.shuffle.partitions", 2).getOrCreate()

input_data = spark_session.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:29092") \
    .option("subscribe", get_input_topic_name()) \
    .option("startingOffsets", "EARLIEST") \
    .load()

input_visits = input_data.selectExpr('CAST(value AS STRING)', 'timestamp') \
    .select(F.from_json(F.col('value'), "userId INT, eventTime TIMESTAMP, browserKey STRING,"
                                        "browserVersion STRING, visitedPage STRING")
            .alias('data')) \
    .selectExpr('data.*')

valid_visits = input_visits.filter('visitedPage IS NOT NULL AND eventTime IS NOT NULL') \
    .withColumnRenamed('visitedPage', 'page')


def write_to_jdbc(dataset: DataFrame, batch_number: int):
    dataset.write.mode('append').format("jdbc") \
        .option("driver", "org.postgresql.Driver") \
        .option("url", "jdbc:postgresql:bde_h6") \
        .option("dbtable", "bde_h6.visits") \
        .option("user", "bde_user") \
        .option("password", "bde_password") \
        .save()


write_query = valid_visits.writeStream.outputMode("update") \
    .option('checkpointLocation', "/tmp/bde/6/e1/checkpoint/stateless") \
    .foreachBatch(write_to_jdbc) \
    .start()

write_query.awaitTermination()
