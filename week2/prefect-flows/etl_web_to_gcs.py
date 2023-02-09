from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from random import randint


@task(retries=3)
def fetch(dataset_url: str) -> pd.DataFrame:
    """Read taxi data from web into pandas DataFrame"""
    # if randint(0, 1) > 0:
    #     raise Exception
    df = pd.read_csv(dataset_url)
    return df


@task(log_prints=True)
def clean(df: pd.DataFrame) -> pd.DataFrame:
    """Fix dtype issues"""
    df["lpep_pickup_datetime"] = pd.to_datetime(df["lpep_pickup_datetime"])
    df["lpep_dropoff_datetime"] = pd.to_datetime(df["lpep_dropoff_datetime"])
    print(df.head(2))
    print(f"columns: {df.dtypes}")
    print(f"rows: {len(df)}")
    return df


@task()
def write_local(local_data_path: str, color: str, dataset_file: str, df: pd.DataFrame) -> None:
    """Write DataFrame out locally as parquet file"""
    df.to_parquet(local_data_path, compression="gzip")
    return


@task()
def write_gcs(local_data_path: str, gcs_data_path: str, color: str, dataset_file: str) -> None:
    """Upload local parquet file to GCS"""
    gcs_block = GcsBucket.load("gcs-bucket-prefect-flows")
    gcs_block.upload_from_path(from_path=local_data_path, to_path=gcs_data_path)
    return


@flow()
def etl_web_to_gcs(
    color: str,
    year: int,
    month: int,
    local_base_path: str,
    gcs_base_path: str
) -> int:
    """The flow with ETL of getting taxi rides dataset and load it to GCS bucket"""
    dataset_file = f"{color}_tripdata_{year}-{month:02}"
    dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{dataset_file}.csv.gz"
    local_full_path = Path(f"{local_base_path}/{color}/{dataset_file}.parquet")
    gcs_full_path = Path(f"{gcs_base_path}/{color}/{dataset_file}.parquet")

    df = fetch(dataset_url)
    df_clean = clean(df)
    write_local(local_full_path, color, dataset_file, df_clean)
    write_gcs(local_full_path, gcs_full_path, color, dataset_file)
    
    return df_clean.shape[0]


@flow(log_prints=True)
def etl_web_to_gcs_base(
    color: str = "green",
    year: int = 2020,
    months: list[int] = [1],
    local_base_path: str = "../prefect-data",
    gcs_base_path: str = "data"
):
    rows_processed = 0
    for month in months:
        rows_processed += etl_web_to_gcs(color, year, month, local_base_path, gcs_base_path)
    print(f"Flow cycle is finished, rows processed: {rows_processed}")


if __name__ == "__main__":
    etl_web_to_gcs_base()
