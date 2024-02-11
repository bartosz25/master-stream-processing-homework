from pyspark.sql.types import StructType, StructField, StringType, TimestampType, LongType, DoubleType, \
    BooleanType


def get_raw_data_schema_to_cleanse() -> StructType:
    return StructType([
        StructField("user_id", LongType(), False),
        StructField("visit_id", StringType(), True),
        StructField("event_time", TimestampType(), False),
        StructField("keep_private", BooleanType(), False),
        StructField("page", StructType([
            StructField("current", StringType(), False), StructField("previous", StringType(), True)
        ]), False),
        StructField("source", StructType([
            StructField("site", StringType(), False), StructField("api_version", StringType(), False)
        ]), False),
        StructField("user", StructType([
            StructField("ip", StringType(), False), StructField("latitude", DoubleType(), False),
            StructField("longitude", DoubleType(), False)
        ]), False),
        StructField("technical", StructType([
            StructField("browser", StringType(), False), StructField("os", StringType(), False),
            StructField("lang", StringType(), False), StructField("network", StringType(), False),
            StructField("device", StructType([
                StructField("type", StringType(), False), StructField("version", StringType(), True)
            ]), False)
        ]), False)
    ])
