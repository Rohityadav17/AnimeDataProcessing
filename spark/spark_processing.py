from pyspark.sql import SparkSession
from pyspark.sql.functions import expr, coalesce, col
import time

# Initialize Spark
spark = SparkSession.builder.appName("AnimeAnalysis").getOrCreate()

# Load CSV with multiline support
df = spark.read.csv(
    "C:/kss/AnimeDataProcessing/data/anime-data.csv",
    header=True,
    inferSchema=True,
    multiLine=True,
    escape='"'
)


# Select required columns
df = df.select("anime_id", "English name", "Favorites")

# Safely cast Favorites to double, invalid values become 0
df = df.withColumn(
    "popular",
    coalesce(expr("try_cast(Favorites as double)"), expr("double(0)"))
)


start_no_cache_1 = time.time()
df.select("anime_id", "English name", "popular").orderBy(col("popular").desc()).show(10, truncate=False)
end_no_cache_1 = time.time()


start_no_cache_2 = time.time()
df.select("anime_id", "English name", "popular").orderBy(col("popular").desc()).show(10, truncate=False)
end_no_cache_2 = time.time()
print(f"⏳ Execution Time (Without Cache, Second Query): {end_no_cache_2 - start_no_cache_2:.2f} seconds")

spark.stop()