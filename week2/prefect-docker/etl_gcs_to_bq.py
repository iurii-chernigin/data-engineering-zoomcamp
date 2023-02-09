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


@task(
    retries=3,
    retry_delay_seconds=[10, 30, 60])
def write_bq(gcp_creds_block_name: str, gcp_project_id: str, trips_df: pd.DataFrame, bq_table: str) -> int:
    """Write dataframe to BigQuery"""
    gcp_creds_block = GcpCredentials.load(gcp_creds_block_name)
    trips_df.to_gbq(
        destination_table=bq_table,
        project_id=gcp_project_id,
        credentials=gcp_creds_block.get_credentials_from_service_account(),
        chunksize=500_000,
        if_exists="append")
    return trips_df.shape[0]


@flow(version=os.getenv("GIT_COMMIT_SHA"))
def etl_gcs_to_bq(
    dataset_color: str,
    dataset_year: int,
    dataset_month: int,
    gcs_bucket_block_name: str,
    gcp_creds_block_name: str,
    gcp_project_id: str,
    bq_table: str
) -> int:
    """
        Main ETL flow to load data to BigQuery
        It returns number of rows that was processed
    """
    # Task launch
    dataset_local_path = extract_from_gcs(gcs_bucket_block_name, dataset_color, dataset_year, dataset_month)
    trips_df = transform(dataset_local_path)
    rows_processed = write_bq(gcp_creds_block_name, gcp_project_id, trips_df, bq_table)
    return rows_processed


@flow(log_prints=True)
def etl_gcs_to_bq_base(
    dataset_color: str = "yellow",
    dataset_year: int = 2021,
    dataset_months: list[int] = [1],
    gcs_bucket_block_name: str = "gcs-bucket-prefect-flows",
    gcp_creds_block_name: str = "gcp-creds-prefect-flows",
    gcp_project_id: str = "secure-analyzer-375018",
    bq_table: str = "zoomcamp.yellow_taxi_rides"
):
    """Parent ETL flow for parametrizing runs"""
    rows_processed = 0
    for dataset_month in dataset_months:
        rows_processed += etl_gcs_to_bq(
            dataset_color,
            dataset_year,
            dataset_month,
            gcs_bucket_block_name,
            gcp_creds_block_name,
            gcp_project_id,
            bq_table)
    print(f"Flow cycle is finished, rows processed: {rows_processed}")


if __name__ == "__main__":
    etl_gcs_to_bq_base()