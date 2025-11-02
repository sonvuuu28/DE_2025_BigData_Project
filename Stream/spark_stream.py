import os
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    BooleanType,
    ArrayType,
)
from pyspark.sql.functions import explode, col, count

# ===== 1. Spark session =====
spark = SparkSession.builder.appName("MusicVotesStream").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# ===== 2. Schema JSON =====
vote_schema = StructType(
    [
        StructField("status", StringType(), True),
        StructField("count", StringType(), True),
        StructField("delay_used", StringType(), True),
        StructField(
            "votes",
            ArrayType(
                StructType(
                    [
                        StructField("user_id", StringType(), True),
                        StructField("song", StringType(), True),
                        StructField("like", BooleanType(), True),
                        StructField("timestamp", StringType(), True),
                    ]
                )
            ),
        ),
    ]
)

# ===== 3. Input & Output folders =====
RAW_DIR = "raw_data/"  # Folder poller ghi file
OUTPUT_DIR = "output/final_snapshot"  # Snapshot cuối
os.makedirs(OUTPUT_DIR, exist_ok=True)

# ===== 4. Đọc stream JSON =====
raw_df = spark.readStream.schema(vote_schema).option("multiLine", True).json(RAW_DIR)

# ===== 5. Explode votes =====
votes_df = raw_df.select(explode(col("votes")).alias("vote")).select(
    col("vote.timestamp").alias("timestamp"),
    col("vote.user_id").alias("user_id"),
    col("vote.song").alias("song"),
    col("vote.like").alias("like"),
)

# ===== 6. Count votes per song =====
song_counts = votes_df.groupBy("song").agg(count("*").alias("total_votes"))
song_counts = song_counts.orderBy(col("total_votes").desc())


# ===== 7. Hàm ghi batch =====
def save_snapshot(df, batch_id):
    # Ghi overwrite → snapshot cuối cùng luôn cập nhật
    df.write.mode("overwrite").csv(OUTPUT_DIR)
    # In console an toàn Windows
    print(f"Batch {batch_id} written to {OUTPUT_DIR}")
    # In realtime tổng votes
    df.show(truncate=False)


# ===== 8. Start streaming =====
query = (
    song_counts.writeStream.outputMode("complete")  # complete để tổng votes
    .foreachBatch(save_snapshot)  # ghi CSV + show console
    .trigger(processingTime="10 seconds")  # đọc file mới mỗi 10s
    .start()
)

# ===== 9. Chờ stop =====
query.awaitTermination()
