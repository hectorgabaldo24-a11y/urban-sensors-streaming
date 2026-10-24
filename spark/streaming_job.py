import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    avg,
    col,
    from_json,
    to_timestamp,
    window,
)
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

KAFKA_BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"
)
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "urban_sensors")
WINDOW_DURATION = os.getenv("SPARK_WINDOW_DURATION", "1 minute")

schema = StructType([
    StructField("sensor_id", StringType(), False),
    StructField("temperature", DoubleType(), True),
    StructField("humidity", DoubleType(), True),
    StructField("air_quality_index", IntegerType(), True),
    StructField("timestamp", StringType(), False),
])

spark = (
    SparkSession.builder
    .appName("UrbanSensorsStreaming")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

events = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
    .option("subscribe", KAFKA_TOPIC)
    .option("startingOffsets", "earliest")
    .load()
)

parsed = (
    events
    .select(from_json(col("value").cast("string"), schema).alias("data"))
    .select("data.*")
    .withColumn("event_time", to_timestamp(col("timestamp")))
    .filter(col("sensor_id").isNotNull())
    .filter(col("event_time").isNotNull())
)

averages = (
    parsed
    .withWatermark("event_time", "2 minutes")
    .groupBy(
        window(col("event_time"), WINDOW_DURATION),
        col("sensor_id"),
    )
    .agg(
        avg("temperature").alias("average_temperature"),
        avg("air_quality_index").alias("average_air_quality_index"),
    )
    .select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("sensor_id"),
        col("average_temperature"),
        col("average_air_quality_index"),
    )
)

query = (
    averages.writeStream
    .outputMode("update")
    .format("console")
    .option("truncate", "false")
    .option("checkpointLocation", os.getenv(
        "SPARK_CHECKPOINT_LOCATION",
        "/tmp/spark-checkpoints/urban-sensors",
    ))
    .start()
)

query.awaitTermination()
