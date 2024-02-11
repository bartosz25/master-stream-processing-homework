import random
import time
import uuid

import pendulum as pendulum
from pyspark import Row
from pyspark.sql import SparkSession, functions

spark = SparkSession.builder.master("local[*]")\
    .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0').getOrCreate()

generation_day = pendulum.now('UTC').subtract(days=10)
while pendulum.now('UTC') > generation_day:
    event_times = []
    for _ in range(0, 7):
        if random.randint(0, 30000) % 20 == 0:
            event_times.append(None)
        else:
            event_times.append(generation_day.isoformat())

    visits_to_send = spark.createDataFrame(data=[
        Row(eventId=str(uuid.uuid4()), visitId=1, userId=1, visitedPage="/home", eventTime=event_times[0]),
        Row(eventId=str(uuid.uuid4()), visitId=1, userId=1, visitedPage="/contact", eventTime=event_times[1]),
        Row(eventId=str(uuid.uuid4()), visitId=1, userId=1, visitedPage="/products", eventTime=event_times[2]),
        Row(eventId=str(uuid.uuid4()), visitId=2, userId=2, visitedPage="/my-account", eventTime=event_times[3]),
        Row(eventId=str(uuid.uuid4()), visitId=2, userId=2, visitedPage="/my-orders", eventTime=event_times[4]),
        Row(eventId=str(uuid.uuid4()), visitId=2, userId=2, visitedPage="/my-orders/order/1", eventTime=event_times[5]),
        Row(eventId=str(uuid.uuid4()), visitId=3, userId=3, visitedPage="/home", eventTime=event_times[6])
    ])

    visits_to_send.select(
            functions.col("visitId").cast("string").alias("key"),
            functions.to_json(functions.struct(['eventId', 'visitId', 'userId', 'visitedPage', 'eventTime'])).alias("value")
    )\
    .write.format("kafka")\
    .option("kafka.bootstrap.servers", "localhost:29092")\
    .option("topic", "visits").save()

    generation_day.add(minutes=1)
    time.sleep(5)
