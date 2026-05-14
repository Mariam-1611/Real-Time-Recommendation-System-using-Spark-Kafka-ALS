from pyspark.sql import SparkSession
from pyspark.ml.recommendation import ALSModel
import time, os

spark = SparkSession.builder \
    .appName("Latency_Benchmark") \
    .config("spark.driver.memory", "4g") \
    .getOrCreate()
spark.sparkContext.setLogLevel("WARN")

MODEL_PATH = os.path.expanduser("~/mini_project3/als_model")
model = ALSModel.load(MODEL_PATH)
print("Model loaded — running latency benchmark\n")

results = []
for user_id in [1, 10, 42, 100, 500, 1000, 2000, 3000, 4000, 5000]:
    user_df = spark.createDataFrame([(user_id,)], ["userId"])
    start = time.time()
    recs = model.recommendForUserSubset(user_df, 5)
    recs.collect()
    latency = (time.time() - start) * 1000
    results.append((user_id, latency))
    print(f"User {user_id:5d} -> {latency:.1f} ms  {'✅ < 5s' if latency < 5000 else '⚠️ > 5s'}")

avg_latency = sum(r[1] for r in results) / len(results)
print(f"\nAverage latency: {avg_latency:.1f} ms")
print(f"Max latency:     {max(r[1] for r in results):.1f} ms")
print(f"Min latency:     {min(r[1] for r in results):.1f} ms")
print(f"\nBonus target (< 5000ms): {'✅ ACHIEVED' if avg_latency < 5000 else '❌ NOT MET'}")
spark.stop()

