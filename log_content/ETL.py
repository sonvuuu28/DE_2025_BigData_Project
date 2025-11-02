from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import *
import os
from pyspark import StorageLevel

jar_path = os.path.abspath("./jars/mysql-connector-j-8.0.33.jar")

spark = (
    SparkSession.builder.appName("final_project")
    .config("spark.driver.memory", "8g")
    .config("spark.jars", jar_path)
    .getOrCreate()
)


def ensure_dir(path):
    """Create folder if not exists"""
    if not os.path.exists(path):
        os.makedirs(path)


def read_json(path_name):
    df = spark.read.json(path_name)
    df = df.select("_source.*")

    df = (
        df.withColumnRenamed("AppName", "app_name")
        .withColumnRenamed("Contract", "contract")
        .withColumnRenamed("Mac", "mac")
        .withColumnRenamed("TotalDuration", "total_duration")
    )

    event_date = path_name.split("\\")[-1].split(".")[0]
    df = df.withColumn("event_date", to_date(lit(event_date), "yyyyMMdd"))
    return df


def add_id(df, name):
    return df.withColumn(name, monotonically_increasing_id())


def categorize_app(df):
    return df.withColumn(
        "category",
        when(
            col("app_name").isin("KPLUS", "RELAX", "CHILD", "CHANNEL", "SPORT"),
            "Kênh truyền hình",
        )
        .when(
            col("app_name").isin("VOD", "FIMS", "BHD"), "Phim và nội dung theo yêu cầu"
        )
        .when(col("app_name") == "Apps", "Ứng dụng")
        .otherwise("Khác"),
    )


def add_metrics_contract(df):
    return df.groupBy("contract").agg(
        sum("total_duration").alias("contract_total_duration"),
        count_distinct("mac").alias("total_device"),
        datediff(to_date(lit("2022-04-30"), "yyyy-MM-dd"), max("event_date")).alias(
            "recency"
        ),
        count_distinct("event_date").alias("frequency"),
    )


def add_quartiles(df, quantile_columns):
    cols = list(quantile_columns.keys())
    quantiles_list = df.approxQuantile(cols, [0.25, 0.5, 0.75], 0.1)

    for idx, col_name in enumerate(cols):
        q1, q2, q3 = quantiles_list[idx]
        score_col = quantile_columns[col_name]

        if col_name == "recency":
            df = df.withColumn(
                score_col,
                when(col(col_name) <= q1, 4)
                .when(col(col_name) <= q2, 3)
                .when(col(col_name) <= q3, 2)
                .otherwise(1),
            )
        else:
            df = df.withColumn(
                score_col,
                when(col(col_name) <= q1, 1)
                .when(col(col_name) <= q2, 2)
                .when(col(col_name) <= q3, 3)
                .otherwise(4),
            )
    return df


def create_dim_app(df):
    df = df.groupBy("app_name").agg(
        sum("total_duration").alias("app_total_duration"),
        count_distinct("contract").alias("app_total_active_contracts"),
    )
    df = categorize_app(df)
    df = add_id(df, "app_id")
    return df.select(
        "app_id",
        "category",
        "app_name",
        "app_total_duration",
        "app_total_active_contracts",
    )


def create_dim_contract(df):
    df = add_metrics_contract(df)
    quantile_cols = {
        "recency": "recency_score",
        "frequency": "frequency_score",
        "contract_total_duration": "duration_score",
    }
    df = add_quartiles(df, quantile_cols)
    df = add_id(df, "contract_id")
    return df.select(
        "contract_id",
        "contract",
        "total_device",
        "recency",
        "frequency",
        "contract_total_duration",
        "recency_score",
        "frequency_score",
        "duration_score",
    )


def create_dim_device(df):
    df = df.select("mac").dropDuplicates()
    df = add_id(df, "device_id")
    return df.select("device_id", "mac")


def create_fact_usage(df, dim_app, dim_contract, dim_device):
    df = (
        df.join(broadcast(dim_app), on=["app_name"], how="left")
        .join(dim_contract, on=["contract"], how="left")
        .join(dim_device, on=["mac"], how="left")
    )

    return df.select(
        "app_id", "contract_id", "device_id", "total_duration", "event_date"
    )


def write_to_disk(df, name):
    base_path = "./data_content"
    ensure_dir(base_path)
    output_path = os.path.join(base_path, name)
    ensure_dir(output_path)
    df.write.mode("overwrite").parquet(output_path)


def write_to_mysql(df, table_name, mode="overwrite"):
    url = "jdbc:mysql://localhost:3306/final_project"
    props = {"user": "root", "password": "12345", "driver": "com.mysql.cj.jdbc.Driver"}
    df.write.jdbc(url=url, table=table_name, mode=mode, properties=props)


def read_all_files():
    project_path = os.getcwd()
    log_content_path = os.path.join(project_path, "log_content")
    list_files = os.listdir(log_content_path)

    union_df = None
    for file_name in list_files:
        path_name = os.path.join("./log_content", file_name)
        df = read_json(path_name)

        if union_df is None:
            union_df = df
        else:
            union_df = union_df.unionByName(df, allowMissingColumns=True)

    return union_df


def main():
    df = read_all_files().persist(StorageLevel.MEMORY_AND_DISK)

    dim_app = create_dim_app(df)
    dim_contract = create_dim_contract(df).persist(StorageLevel.MEMORY_AND_DISK)
    dim_device = create_dim_device(df)

    fact_usage = create_fact_usage(df, dim_app, dim_contract, dim_device).persist(
        StorageLevel.MEMORY_AND_DISK
    )

    # Save to Parquet
    write_to_disk(dim_app, "dim_app")
    write_to_disk(dim_contract, "dim_contract")
    write_to_disk(dim_device, "dim_device")
    write_to_disk(fact_usage, "fact_usage")

    # Save to MySQL
    write_to_mysql(dim_app, "dim_app", "overwrite")
    write_to_mysql(dim_contract, "dim_contract", "overwrite")
    write_to_mysql(dim_device, "dim_device", "overwrite")
    write_to_mysql(fact_usage, "fact_usage", "append")

    df.unpersist()
    dim_contract.unpersist()
    fact_usage.unpersist()
    print("✅ Data warehouse written successfully.")


if __name__ == "__main__":
    main()
