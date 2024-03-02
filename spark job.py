from pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
    .appName("Streaming from Kafka") \
    .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0,org.apache.iceberg:iceberg-spark3-runtime:0.12.0') \
    .config("spark.sql.shuffle.partitions", 4) \
    .config("spark.sql.catalog.event.type", "hive") \
    .config("spark.sql.catalog.event.uri", "thrift://localhost:9083") \
    .config("spark.sql.catalog.event.clients", "10") \
    .config("spark.sql.catalog.event.warehouse.dir", "/user/hive/warehouse") \
    .getOrCreate()

df = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "localhost:9092") \
  .option("subscribe", "new_topic") \
  .option("startingOffsets", "earliest") \
  .load()

base_df = df.selectExpr("CAST(value as STRING)")

from pyspark.sql.types import *
from pyspark.sql.functions import *

sample_schema = StructType([
    StructField("EventName", StringType(), True),
    StructField("EventType", StringType(), True),
    StructField("EventValue", IntegerType(), True),
    StructField("EventPageSource", StringType(), True),
    StructField("EventPageURL", StringType(), True),
    StructField("ComponentID", IntegerType(), True),
    StructField("UserID", IntegerType(), True)
])

info_dataframe = base_df.select(
        from_json(col("value"), sample_schema).alias("sample"))

info_df_fin = info_dataframe.select("sample.*")

# Write data to Iceberg table
writing_df = info_df_fin.writeStream \
    .format("iceberg") \
    .outputMode("append") \
    .option("path", "/mnt/d/output/iceberg_table") \
    .option("checkpointLocation", "/mnt/d/sample/checkpoint") \
    .start()

writing_df.awaitTermination()