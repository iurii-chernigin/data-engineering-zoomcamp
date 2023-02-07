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


@task
def write_bq(gcp_creds_blockname: str, gcp_project_id: str, trips_df: pd.DataFrame, bq_table: str) -> None:
    """Write dataframe to BigQuery"""
    gcp_creds_block = GcpCredentials.load(gcp_creds_block)
    pd.to_gbq(
        destination_table=bq_table,
        project_id=gcp_project_id,
        credentials=gcp_cresd_block.get_credentials_from_service_account(),
        chunk_size=500_000,
        if_exists="append"
    )
    

@flow(
    name="Uploading trip data to BigQuery",
    description="Get Green Taxi parquet from GCS bucket, clean, and load to BigQuery",
    version=os.getenv("GIT_COMMIT_SHA"))
def etl_gcs_to_bq():
    """Main ETL flow to load data to BigQuery"""
    # Sorce dataset parameters
    dataset_color = "yellow"
    dataset_year = 2021
    dataset_month = 1
    # Destination parameters of dataset
    gcs_bucket_block_name = "gcs-bucket-prefect-flows"
    gcp_creds_block_name = "gcp-creds-prefect-flows"
    gcp_project_id = "secure-analyzer-375018"
    bq_table = "zoomcamp.yellow_taxi_rides"

    # Task launch
    dataset_local_path = extract_from_gcs(gcs_bucket_block_name, dataset_color, dataset_year, dataset_month)
    trips_df = transform(dataset_local_path)
    write_bq(gcp_creds_block_name, trips_df, bq_table)


if __name__ == "__main__":
    etl_gcs_to_bq()