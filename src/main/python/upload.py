import os
import logging
import time
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit

CYCLES_DELAY_TIME = 1

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

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
    spark.sparkContext.setLogLevel("ERROR")
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

    for index, (process_date, local_path) in enumerate(all_days):
        upload_day_data(spark, local_path, process_date, incremental_hotel_weather_path)
        logger.info(f"Uploaded data for {process_date.strftime('%Y-%m-%d')}")
        logger.info(
            f"Processing {index + 1}/{len(all_days)} completed. Sleeping for {CYCLES_DELAY_TIME} seconds."
        )
        time.sleep(CYCLES_DELAY_TIME)


def main() -> None:
    spark = conf_spark()
    incremental_hotel_weather_path = f"abfss://{storage_container_name}@{storage_account_name}.dfs.core.windows.net/incremental-hotel-weather"
    base_local_path = os.path.abspath(
        os.path.join(os.path.dirname(__file__), "../../../m13sparkstreaming/hotel-weather")
    )

    incremental_upload(base_local_path, spark, incremental_hotel_weather_path)


if __name__ == "__main__":
    main()
