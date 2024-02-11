from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local[*]")\
    .config('spark.jars.packages', 'io.delta:delta-core_2.12:2.2.0')\
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")\
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")\
    .getOrCreate()

print('======== TOP 20 OF VALID RECORDS ============ ')
spark.read.format("delta").load("/tmp/bde-3/homework/p2/e1/output/valid").show(truncate=False)

print('======== TOP 20 OF INVALID RECORDS ============ ')
spark.read.format("delta").load("/tmp/bde-3/homework/p2/e1/output/invalid").show(truncate=False)