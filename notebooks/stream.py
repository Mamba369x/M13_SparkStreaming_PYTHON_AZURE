import os
import logging
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    approx_count_distinct,
    avg,
    max,
    min,
    row_number,
)
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, DateType

# Delay in seconds
DELAY_SECONDS = 20
batch_count = 0  # Global counter for batches

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
base_local_path = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "../m13sparkstreaming/hotel-weather")
)

# Check if the base local path exists
if not os.path.exists(base_local_path):
    raise FileNotFoundError(f"The path {base_local_path} does not exist.")

# Initialize Spark session with Delta extensions
spark = (
    SparkSession.builder.appName("IncrementalProcessing")
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

# Define schema
schema = StructType(
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

# Read the streaming data from the parquet files
df = (
    spark.readStream.format("parquet")
    .schema(schema)
    .load(incremental_hotel_weather_path)
)

# Convert string date to date type
df = df.withColumn("wthr_date", col("wthr_date").cast(DateType()))

# Perform aggregations
result_df = df.groupBy("city", "wthr_date").agg(
    approx_count_distinct("id").alias("distinct_hotels"),
    avg("avg_tmpr_c").alias("avg_temperature"),
    max("avg_tmpr_c").alias("max_temperature"),
    min("avg_tmpr_c").alias("min_temperature"),
)

# Initialize an empty DataFrame to collect results
final_df = None


# Function to process and collect each batch
def process_and_collect(batch_df, epoch_id):
    global final_df, batch_count
    if final_df is None:
        final_df = batch_df
    else:
        final_df = final_df.union(batch_df)

    batch_count += 1  # Increment the batch counter

    # Calculate the statistics for each batch
    batch_df.show(truncate=False)
    logger.info(f"Processed batch {batch_count}")


# Write the results to the console for debugging (you can also write to a Delta table)
query = (
    result_df.writeStream.outputMode("append").foreachBatch(process_and_collect).start()
)

try:
    while True:
        if not query.status["isDataAvailable"]:
            logger.info("No new data found. Sleeping for {DELAY_SECONDS} seconds.")
            time.sleep(DELAY_SECONDS)
            if not query.status["isDataAvailable"]:
                print("No new data was available after sleep therefore exiting stream")
                break
except KeyboardInterrupt:
    logger.info("Terminating the stream...")

# Process the final DataFrame after termination
if final_df is not None:
    # Define window specification to consistently order results
    window_spec = Window.orderBy(col("distinct_hotels").desc(), col("wthr_date").desc())

    # Add row numbers to the DataFrame to keep only top 10
    final_df = final_df.withColumn("row_number", row_number().over(window_spec))
    top_10_df = final_df.filter(col("row_number") <= 10).drop("row_number")

    # Show the top 10 rows
    top_10_df.show(truncate=False)

    # Convert to Pandas DataFrame for further processing
    pandas_df = top_10_df.toPandas()

    # Reorder the columns to make 'distinct_hotels' the first column
    columns = [
        "distinct_hotels",
        "city",
        "wthr_date",
        "avg_temperature",
        "max_temperature",
        "min_temperature",
    ]
    pandas_df = pandas_df[columns]

    # Save the final DataFrame to a CSV file
    output_path = "top_10.csv"
    pandas_df.to_csv(output_path, index=False)

    logger.info("Final sorted dataframe schema:")
    logger.info(pandas_df.dtypes)
else:
    logger.info("No data was processed.")
