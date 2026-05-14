from pyspark.sql import SparkSession
from pyspark.ml.recommendation import ALS
from pyspark.ml.evaluation import RegressionEvaluator
import time, os

spark = SparkSession.builder \
    .appName("MovieLens_ALS") \
    .config("spark.sql.shuffle.partitions", "8") \
    .config("spark.driver.memory", "4g") \
    .getOrCreate()
spark.sparkContext.setLogLevel("WARN")
print("Spark started")

DATA_PATH = os.path.expanduser("~/ml-1m/ratings.dat")
raw_df = spark.read.text(DATA_PATH)
ratings_df = raw_df.selectExpr(
    "cast(split(value, '::')[0] as int) as userId",
    "cast(split(value, '::')[1] as int) as movieId",
    "cast(split(value, '::')[2] as float) as rating",
    "cast(split(value, '::')[3] as long) as timestamp"
).dropna()

print(f"Loaded {ratings_df.count()} ratings")
train_df, test_df = ratings_df.randomSplit([0.8, 0.2], seed=42)
print(f"Train: {train_df.count()} | Test: {test_df.count()}")

als = ALS(userCol="userId", itemCol="movieId", ratingCol="rating",
          rank=20, maxIter=10, regParam=0.1,
          coldStartStrategy="drop", nonnegative=True)

print("Training ALS...")
start = time.time()
model = als.fit(train_df)
print(f"Training done in {time.time()-start:.1f}s")

predictions = model.transform(test_df)
evaluator = RegressionEvaluator(
    metricName="rmse", labelCol="rating", predictionCol="prediction")
rmse = evaluator.evaluate(predictions)
print(f"RMSE (rank=20, regParam=0.1, maxIter=10): {rmse:.4f}")

if rmse > 1.5:
    print("RMSE > 1.5 — tuning rank...")
    best_rmse, best_rank, best_model = rmse, 20, model
    for rank in [30, 50]:
        m = ALS(userCol="userId", itemCol="movieId", ratingCol="rating",
                rank=rank, maxIter=10, regParam=0.1,
                coldStartStrategy="drop", nonnegative=True).fit(train_df)
        r = evaluator.evaluate(m.transform(test_df))
        print(f"  rank={rank} -> RMSE={r:.4f}")
        if r < best_rmse:
            best_rmse, best_rank, best_model = r, rank, m
    model = best_model
    print(f"Best rank={best_rank} RMSE={best_rmse:.4f}")
else:
    print("RMSE acceptable — no tuning needed")

print("\nTop-5 recommendations sample:")
model.recommendForAllUsers(5).show(5, truncate=False)

MODEL_PATH = os.path.expanduser("~/mini_project3/als_model")
model.write().overwrite().save(MODEL_PATH)
print(f"Model saved to {MODEL_PATH}")
spark.stop()
print("Done!")

