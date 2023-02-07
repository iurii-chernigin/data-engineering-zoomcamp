from pathlib import Path
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials 
import os
import pandas as pd


@task(retries=3, retry_delay_seconds=[10, 30, 60], log_prints=True)
def extract_from_gcs(gcs_block_name: str, color: str, year: int, month: int) -> Path:
    """Download trip data from GCS bucket"""
    gcs_path = f"data/{color}/{color}_tripdata_{year}-{month:02}.parquet"
    print(f"extract_from_gcp func, path to gcs: {gcs_path}")
    gcs_block = GcsBucket.load(gcs_block_name)
    gcs_block.get_directory(from_path=gcs_path, local_path="./")
    return gcs_path

@task(
    retries=3, 
    retry_delay_seconds=[10, 30, 60],
    log_prints=True)
def transform(path: str) -> pd.DataFrame:
    """Data Cleaning"""
    df = pd.read_parquet(path)
    
    na_passangers_count = df["passenger_count"].isna().sum()
    print(f"Pre-transform: rows with null value in passenger_count field: {na_passangers_count}")
    
    df["passenger_count"].fillna(0, inplace=True)
    na_passangers_count = df["passenger_count"].isna().sum()
    print(f"Post-transform: rows with null value in passenger_count field: {na_passangers_count}")

    return df

@flow(
    name="Uploading trip data to BigQuery",
    description="Get Green Taxi parquet from GCS bucket, clean, and load to BigQuery",
    version=os.getenv("GIT_COMMIT_SHA"))
def etl_gcs_to_bq():
    """Main ETL flow to load data to BigQuery"""
    color = "yellow"
    year = 2021
    month = 1

    gcs_block_name = "gcs-bucket-prefect-flows"

    path = extract_from_gcs(gcs_block_name, color, year, month)
    df = transform(path)


if __name__ == "__main__":
    etl_gcs_to_bq()