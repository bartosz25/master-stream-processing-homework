from pyspark.sql import DataFrame, functions as F
from pyspark.sql.types import StructType, StructField, StringType

user_visit_schema = StructType([
    StructField("os", StringType()),
    StructField("visitedPage", StringType())
])


def process_input_data(input_data_frame: DataFrame) -> DataFrame:
    return input_data_frame.select(
        F.col("key").cast("string"),
        F.from_json(F.col("value").cast("string"), user_visit_schema).alias("visit"),
        F.col("value").cast("string")
    ).filter("visit.os IN ('macOS', 'Windows', 'Linux')")\
        .filter("visit.visitedPage != ''") \
    .select(
        F.col("key"), F.col("value")
    )
