import os
import logging
import time
from datetime import datetime
from threading import Thread
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col,
    approx_count_distinct,
    avg,
    max,
    min,
    row_number,
    lit,
)
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, DateType

STREAM_WAIT_SECONDS = 10
SLEEP = 5
CYCLES = 3

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

final_df: DataFrame = None

storage_account_name = os.getenv("STORAGE_ACCOUNT_NAME")
storage_container_name = os.getenv("STORAGE_CONTAINER_NAME")
tf_var_client_id = os.getenv("TF_VAR_CLIENT_ID")
tf_var_client_secret = os.getenv("TF_VAR_CLIENT_SECRET")
tf_var_tenant_id = os.getenv("TF_VAR_TENANT_ID")
if not all(
    [
        storage_account_name,
        storage_container_name,
        tf_var_client_id,
        tf_var_client_secret,
        tf_var_tenant_id,
    ]
):
    raise ValueError("One or more environment variables are missing")


def conf_spark() -> SparkSession:
    spark = (
        SparkSession.builder.appName("IncrementalProcessing")
        .config("spark.sql.parquet.int96RebaseModeInWrite", "LEGACY")
        .getOrCreate()
    )
    spark.conf.set(
        f"fs.azure.account.auth.type.{storage_account_name}.dfs.core.windows.net",
        "OAuth",
    )
    spark.conf.set(
        f"fs.azure.account.oauth.provider.type.{storage_account_name}.dfs.core.windows.net",
        "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
    )
    spark.conf.set(
        f"fs.azure.account.oauth2.client.id.{storage_account_name}.dfs.core.windows.net",
        tf_var_client_id,
    )
    spark.conf.set(
        f"fs.azure.account.oauth2.client.secret.{storage_account_name}.dfs.core.windows.net",
        tf_var_client_secret,
    )
    spark.conf.set(
        f"fs.azure.account.oauth2.client.endpoint.{storage_account_name}.dfs.core.windows.net",
        f"https://login.microsoftonline.com/{tf_var_tenant_id}/oauth2/token",
    )
    return spark


def define_schema() -> StructType:
    return StructType(
        [
            StructField("address", StringType(), True),
            StructField("avg_tmpr_c", DoubleType(), True),
            StructField("avg_tmpr_f", DoubleType(), True),
            StructField("city", StringType(), True),
            StructField("country", StringType(), True),
            StructField("geoHash", StringType(), True),
            StructField("id", StringType(), True),
            StructField("latitude", DoubleType(), True),
            StructField("longitude", DoubleType(), True),
            StructField("name", StringType(), True),
            StructField("wthr_date", StringType(), True),
            StructField("wthr_year", StringType(), True),
            StructField("wthr_month", StringType(), True),
            StructField("wthr_day", StringType(), True),
        ]
    )


def upload_day_data(
    spark: SparkSession,
    local_path: str,
    process_date: datetime,
    incremental_hotel_weather_path: str,
) -> None:
    df = spark.read.parquet(local_path)
    df = df.withColumn("processed_date", lit(process_date))
    output_path = os.path.join(
        incremental_hotel_weather_path,
        f"year={process_date.year}",
        f"month={process_date.month:02d}",
        f"day={process_date.day:02d}",
    )
    df.write.mode("overwrite").parquet(output_path)
    logger.info(
        f"Data for {process_date.strftime('%Y-%m-%d')} has been processed and uploaded to {output_path}"
    )


def incremental_upload(
    base_local_path: str,
    spark: SparkSession,
    incremental_hotel_weather_path: str,
) -> None:
    all_days = []
    for root, dirs, files in os.walk(base_local_path):
        for dir_name in sorted(dirs):
            if "day=" in dir_name:
                date_str = dir_name.split("=")[-1]
                month_str = root.split("/")[-1].split("=")[-1]
                year_str = root.split("/")[-2].split("=")[-1]
                process_date = datetime.strptime(
                    f"{year_str}-{month_str}-{date_str}", "%Y-%m-%d"
                )
                local_path = os.path.join(root, dir_name)
                all_days.append((process_date, local_path))

    all_days.sort(key=lambda x: x[0])

    for cycle in range(CYCLES):
        if cycle < len(all_days):
            process_date, local_path = all_days[cycle]
            upload_day_data(
                spark, local_path, process_date, incremental_hotel_weather_path
            )
            logger.info(f"Uploaded data for {process_date.strftime('%Y-%m-%d')}")
            logger.info(f"Cycle {cycle + 1}/{CYCLES} completed. Sleeping for {SLEEP}")
            time.sleep(SLEEP)


def process_and_collect(batch_df: DataFrame, epoch_id: int) -> None:
    global final_df
    if final_df is None:
        final_df = batch_df
    else:
        for col_name in final_df.columns:
            if col_name not in batch_df.columns:
                batch_df = batch_df.withColumn(
                    col_name, lit(None).cast(final_df.schema[col_name].dataType)
                )
        final_df = final_df.union(batch_df)
    batch_df.show(truncate=False)
    logger.info("Processed batch")


def start_streaming(
    spark: SparkSession,
    schema: StructType,
    incremental_hotel_weather_path: str,
) -> None:
    while True:
        if not os.path.exists(incremental_hotel_weather_path):
            logger.info(
                f"Path {incremental_hotel_weather_path} does not exist. Sleeping for {STREAM_WAIT_SECONDS} seconds."
            )
            time.sleep(STREAM_WAIT_SECONDS)
            continue

        df = (
            spark.readStream.format("parquet")
            .schema(schema)
            .load(incremental_hotel_weather_path)
        )

        df = df.withColumn("wthr_date", col("wthr_date").cast(DateType()))
        result_df = df.groupBy("city", "wthr_date").agg(
            approx_count_distinct("id").alias("distinct_hotels"),
            avg("avg_tmpr_c").alias("avg_temperature"),
            max("avg_tmpr_c").alias("max_temperature"),
            min("avg_tmpr_c").alias("min_temperature"),
        )

        query = (
            result_df.writeStream.outputMode("append")
            .foreachBatch(process_and_collect)
            .start()
        )

        query.awaitTermination()
        break


def save_final_results() -> None:
    global final_df
    window_spec = Window.orderBy(col("distinct_hotels").desc(), col("wthr_date").desc())
    final_df = final_df.withColumn("row_number", row_number().over(window_spec))
    top_10_df = final_df.filter(col("row_number") <= 10).drop("row_number")
    top_10_df.show(truncate=False)
    pandas_df = top_10_df.toPandas()
    columns = [
        "distinct_hotels",
        "city",
        "wthr_date",
        "avg_temperature",
        "max_temperature",
        "min_temperature",
    ]
    pandas_df = pandas_df[columns]
    output_path = "top_10.csv"
    pandas_df.to_csv(output_path, index=False)
    logger.info("Final sorted dataframe schema:")
    logger.info(pandas_df.dtypes)


def main() -> None:
    spark = conf_spark()
    schema = define_schema()

    incremental_hotel_weather_path = f"abfss://{storage_container_name}@{storage_account_name}.dfs.core.windows.net/incremental-hotel-weather"
    base_local_path = os.path.abspath(
        os.path.join(os.path.dirname(__file__), "../m13sparkstreaming/hotel-weather")
    )

    global final_df
    final_df = None

    logger.info("Starting incremental upload cycles in a separate thread...")
    upload_thread = Thread(
        target=incremental_upload,
        args=(base_local_path, spark, incremental_hotel_weather_path),
    )
    upload_thread.start()

    logger.info("Starting streaming process in the main thread...")
    start_streaming(spark, schema, incremental_hotel_weather_path)

    if upload_thread.is_alive():
        upload_thread.join()

    save_final_results()


if __name__ == "__main__":
    main()
