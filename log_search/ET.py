from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import *
import os


spark = (
    SparkSession.builder.appName("final_project")
    .config("spark.driver.memory", "8g")
    .getOrCreate()
)


def read_files(month):
    folders = os.listdir("./log_search/")
    df = None
    for folder in folders:
        if int(folder[4:6]) == month:
            if df is None:
                df = spark.read.parquet(os.path.join("./log_search", folder))
            else:
                df_next = spark.read.parquet(os.path.join("./log_search", folder))
                df = df.union(df_next)

    return df


def filter_most_search(month):
    df = read_files(month)
    df = df.select("user_id", "keyword")
    df = df.groupBy("user_id", "keyword").count()
    win_spec = Window.partitionBy("user_id").orderBy(col("count").desc())
    df = df.withColumn("rank", row_number().over(win_spec))
    df = df.filter(col("rank") == 1)
    df = df.withColumnRenamed("keyword", "most_search")
    df = df.select("user_id", "most_search")
    return df


def write_to_disk(df, month):
    path = f"./output/t{month}"
    if not os.path.exists(path):
        os.makedirs(path)
    df.write.mode("overwrite").option("header", "True").csv(path)


def main():
    t6 = filter_most_search(6)
    t7 = filter_most_search(7)
    write_to_disk(t6, 6)
    write_to_disk(t7, 7)
    print("✅ Written successfully.")


main()
