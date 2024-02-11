import functools
import sys
from datetime import datetime, timedelta

from pyspark.sql import SparkSession

from exercise2.configuration import get_files_output_dir, get_sink_configuration
from exercise2.streaming_job_processing_logic import process_input_data


def reprocess_data_between_dates(start_date: datetime, end_date: datetime):
    spark = SparkSession.builder.master("local[*]") \
        .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0') \
        .getOrCreate()

    partition_dataframes_to_reprocess = []
    current_date = start_date
    while current_date <= end_date:
        event_time = str(current_date.strftime('%Y-%m-%d'))
        partition_dataframes_to_reprocess.append(
            spark.read.schema("key STRING, value STRING").json(f"{get_files_output_dir()}/event_time={event_time}")
        )
        current_date += timedelta(days=1)

    final_dataframe_to_reprocess = functools.reduce(
        lambda all_partitions, partition_df: all_partitions.union(partition_df),
        partition_dataframes_to_reprocess)

    records_to_write = process_input_data(final_dataframe_to_reprocess)

    records_to_write.write.format("kafka") \
        .options(**get_sink_configuration()) \
        .save()


if __name__ == '__main__':
    start_date = datetime.strptime(sys.argv[1], '%Y-%m-%d')
    end_date = datetime.strptime(sys.argv[2], '%Y-%m-%d')
    reprocess_data_between_dates(start_date, end_date)
