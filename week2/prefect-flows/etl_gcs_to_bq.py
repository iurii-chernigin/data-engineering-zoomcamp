from pathlib import Path
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials 
import os
import pandas as pd


@task(
    retries=3, 
    retry_delay_seconds=[10, 30, 60],
    log_prints=True)
def extract_from_gcs(gcs_block_name: str, color: str, year: int, month: int) -> Path:
    """Download trip data from GCS bucket"""
    gcs_path = f"data/{color}/{color}_tripdata_{year}_{month:02}.parquet"
    gcs_block = GcsBucket.load(gcs_block_name)
    gcs_block.get_directory(from_path=gcs_path, local_path=gcs_path)
    return 


@flow(
    name="Uploading trip data to BigQuery",
    description="Get Green Taxi parquet from GCS bucket, clean, and load to BigQuery",
    version=os.getenv("GIT_COMMIT_SHA")
)
def etl_gcs_to_bq():
    """Main ETL flow to load data to BigQuery"""
    color = "yellow"
    year = 2021
    month = 1

    gcs_block_name = "gcs-bucket-prefect-flows"

    path = extract_from_gcs(gcs_block_name, color, year, month)


if __name__ == "__main__":
    etl_gcs_to_bq()