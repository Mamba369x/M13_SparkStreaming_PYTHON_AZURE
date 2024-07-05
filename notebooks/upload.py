import os
import logging
import time
import json
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit

# Delay in seconds
DELAY_SECONDS = 15
CYCLES = 150
cycle_count = 0  # Global counter for cycles

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Load variables from environment
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

# Define paths
incremental_hotel_weather_path = f"abfss://{storage_container_name}@{storage_account_name}.dfs.core.windows.net/incremental-hotel-weather"
base_local_path = os.path.join(
    os.path.dirname(__file__), "../m13sparkstreaming/hotel-weather"
)

# Check if the base local path exists
if not os.path.exists(base_local_path):
    raise FileNotFoundError(f"The path {base_local_path} does not exist.")

# Initialize Spark session with Delta extensions
spark = (
    SparkSession.builder.appName("AzureDataLakeGen2Connection")
    .config("spark.sql.parquet.int96RebaseModeInWrite", "LEGACY")
    .getOrCreate()
)

# Set the Spark configuration to use the Azure Data Lake Storage Gen2 credentials
spark.conf.set(
    f"fs.azure.account.auth.type.{storage_account_name}.dfs.core.windows.net", "OAuth"
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


# Function to get the path for the date to be processed
def get_date_path(base_path, date):
    year = date.year
    month = f"{date.month:02d}"
    day = f"{date.day:02d}"
    return os.path.join(base_path, f"year={year}", f"month={month}", f"day={day}")


# Function to upload data for a specific day
def upload_day_data(local_path, process_date, time_dict):
    start_time = time.time()

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

    end_time = time.time()
    time_dict[process_date.strftime("%Y-%m-%d")] = end_time - start_time


# Iterate over the folders and upload data incrementally
time_dict = {}

for root, dirs, files in os.walk(base_local_path):
    for dir_name in sorted(dirs):
        if "day=" in dir_name and cycle_count < CYCLES:
            # Extract the date from the folder name
            date_str = dir_name.split("=")[-1]
            month_str = root.split("/")[-1].split("=")[-1]
            year_str = root.split("/")[-2].split("=")[-1]
            print(
                f"Parsing date from year: {year_str}, month: {month_str}, day: {date_str}"
            )
            try:
                process_date = datetime.strptime(
                    f"{year_str}-{month_str}-{date_str}", "%Y-%m-%d"
                )
            except ValueError as e:
                print(f"Error parsing date: {e}")
                continue
            local_path = os.path.join(root, dir_name)
            print(f"Processing {local_path} for date {process_date}")

            # Upload the data for this day
            upload_day_data(local_path, process_date, time_dict)

            # Delay before the next cycle
            logger.info(f"Sleeping for {DELAY_SECONDS}")
            time.sleep(DELAY_SECONDS)

            cycle_count += 1

# Save the time taken for each upload to a JSON file
with open("upload_times.json", "w") as f:
    json.dump(time_dict, f)
print(len(time_dict.keys()))
logger.info("Upload times have been saved to upload_times.json")
