# Spark SQL

Link to project repo - https://github.com/Mamba369x/M13_SparkStreaming_PYTHON_AZURE/tree/main

## Prerequisites

- Azure CLI
- Databricks CLI
- Terraform
- Python 3
- wget (for Linux/Mac) or PowerShell (for Windows)
- Make
- Jq

## Environment Variables

Before running the Makefile targets, ensure that the following environment variables are set:

- `TF_VAR_SUBSCRIPTION_ID`: Your Azure subscription ID

### Setting Environment Variables

#### On Mac and Linux

To set the environment variables on Mac or Linux, you can use the `export` command in the terminal:

```bash
export TF_VAR_SUBSCRIPTION_ID=<your_subscription_id>
```

## Example Usage

* Step 1: First step involes creating terraform infrastructure and databricks cluster



```bash
make start
```

![Step 1:](src/terraform_created.png)

* Step 2:

![Step 2:](src/databricks_job_started.png)

df = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "parquet")
    .schema(schema)
    .load(input_path)
)