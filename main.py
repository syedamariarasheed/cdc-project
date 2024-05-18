import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import *


# Create a SparkSession
spark = SparkSession.builder \
    .appName("Spark Example MinIO") \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.0,io.delta:delta-core_2.12:2.4.0") \
    .config("spark.hadoop.fs.s3a.access.key", "admin") \
    .config("spark.hadoop.fs.s3a.secret.key", "secret123") \
    .config("spark.hadoop.fs.s3a.path.style.access", True) \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio-sinem:9000") \
    .getOrCreate()

spark.sparkContext.setLogLevel('ERROR')

# Define Kafka configurations
kafka_bootstrap_servers = "kafka:9092"
kafka_topic = "postgres.public.cars"


customerFields = [
    StructField("model", StringType()),
    StructField("year", StringType()),
    StructField("brand", StringType()),
]

# Define the schema for the Kafka messages
schema = StructType([
    StructField("payload", StructType([
        StructField("before", StructType(customerFields)),
        StructField("after", StructType(customerFields)),
        StructField("ts_ms", StringType()),
        StructField("op", StringType())
    ]))
])

# Read data from Kafka
kafka_df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", kafka_topic) \
    .load()

# Convert Kafka message value from binary to string
kafka_df = kafka_df \
    .selectExpr("CAST(value AS STRING)")

# Parse JSON data
parsed_df = kafka_df \
    .select(from_json("value", schema).alias("data")) \
    .select("data.*")

# Write data to MinIO (S3)
query = parsed_df \
    .writeStream \
    .format("parquet") \
    .outputMode("append") \
    .option("path", "s3a://my-test-bucket/") \
    .option("checkpointLocation", "s3a://my-test-bucket/checkpoint") \
    .start()

# Wait for the query to terminate
query.awaitTermination()
#
# # Stop the SparkSession
# spark.stop()
