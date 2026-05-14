from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.ml.recommendation import ALSModel
import os

spark = SparkSession.builder \
    .appName("MovieLens_Streaming") \
    .config("spark.sql.shuffle.partitions", "4") \
    .config("spark.driver.memory", "4g") \
    .getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# Load saved ALS model
MODEL_PATH = os.path.expanduser("~/mini_project3/als_model")
model = ALSModel.load(MODEL_PATH)
print("ALS model loaded")

# Schema for incoming Kafka JSON events
schema = StructType([
    StructField("user_id",   IntegerType(), True),
    StructField("item_id",   IntegerType(), True),
    StructField("rating",    FloatType(),   True),
    StructField("timestamp", StringType(),  True)
])

# Read from Kafka
raw_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "movie-ratings") \
    .option("startingOffsets", "latest") \
    .load()

# Parse JSON safely — malformed records become nulls and are dropped
parsed = raw_stream.select(
    from_json(col("value").cast("string"), schema).alias("data"),
    col("timestamp").alias("kafka_time")
).select("data.*", "kafka_time") \
 .filter(col("user_id").isNotNull() & col("item_id").isNotNull() & col("rating").isNotNull())

# Add watermark for late data handling (2 minutes late tolerance)
events = parsed.withColumn("event_time", to_timestamp(col("timestamp"))) \
               .withWatermark("event_time", "2 minutes")

# Window Analytics — 30 second window, sliding every 10 seconds
window_analytics = events \
    .groupBy(
        window(col("event_time"), "30 seconds", "10 seconds"),
        col("item_id")
    ).agg(
        avg("rating").alias("avg_rating"),
        count("*").alias("interaction_count"),
        (avg("rating") * log(count("*") + 1)).alias("trending_score")
    )

# Alert: items with avg_rating > 4.5 are flagged as trending
alerts = window_analytics.filter(col("avg_rating") > 4.5)

# Top-5 recommendations for incoming users
user_stream = parsed.select(col("user_id").alias("userId")).distinct()
recommendations = model.recommendForUserSubset(user_stream, 5)

# Output 1: Window analytics to console
query1 = window_analytics.writeStream \
    .outputMode("update") \
    .format("console") \
    .option("truncate", False) \
    .trigger(processingTime="10 seconds") \
    .start()

# Output 2: Alerts to console
query2 = alerts.writeStream \
    .outputMode("update") \
    .format("console") \
    .option("truncate", False) \
    .trigger(processingTime="10 seconds") \
    .start()

print("Streaming started — waiting for events from Kafka...")
print("Window: 30s | Slide: 10s | Watermark: 2min")
spark.streams.awaitAnyTermination()

