from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import os

jar_path = os.path.abspath("./jars/mysql-connector-j-8.0.33.jar")

spark = (
    SparkSession.builder.appName("final_project")
    .config("spark.driver.memory", "8g")
    .config("spark.jars", jar_path)
    .getOrCreate()
)


def load_data():
    t6 = spark.read.csv("./output_AI/t6.csv", header=True)
    t7 = spark.read.csv("./output_AI/t7.csv", header=True)
    return t6, t7


def preprocess(t6, t7):
    t6 = t6.withColumnRenamed("most_search", "most_search_t6").withColumnRenamed(
        "category", "category_t6"
    )

    t7 = t7.withColumnRenamed("most_search", "most_search_t7").withColumnRenamed(
        "category", "category_t7"
    )

    return t6, t7


def analyze_trending(t6, t7):
    df = t6.join(t7, on="user_id", how="outer")

    df = df.withColumn(
        "trending_type",
        when(col("category_t6") == col("category_t7"), "Unchanged").otherwise(
            "Changed"
        ),
    )

    df = df.withColumn(
        "previous",
        when(col("trending_type") == "Unchanged", "Unchanged").otherwise(
            concat_ws("->", col("category_t6"), col("category_t7"))
        ),
    )
    return df


def write_to_mysql(df, table_name, mode="overwrite"):
    url = "jdbc:mysql://localhost:3306/final_project"
    props = {"user": "root", "password": "12345", "driver": "com.mysql.cj.jdbc.Driver"}
    df.write.jdbc(url=url, table=table_name, mode=mode, properties=props)


def main():
    t6, t7 = load_data()
    t6, t7 = preprocess(t6, t7)
    df_final = analyze_trending(t6, t7)

    write_to_mysql(df_final, "fact_trending_change", "overwrite")


main()
