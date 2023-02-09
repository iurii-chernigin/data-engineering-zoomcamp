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
def write_local(local_data_path: str, color: str, dataset_file: str, df: pd.DataFrame) -> Path:
    """Write DataFrame out locally as parquet file"""
    path = Path(f"{local_data_path}/{color}/{dataset_file}.parquet")
    df.to_parquet(path, compression="gzip")
    return path


@task()
def write_gcs(gcs_data_path: str, color: str, dataset_file: str) -> None:
    """Upload local parquet file to GCS"""
    gcs_block = GcsBucket.load("gcs-bucket-prefect-flows")
    path = Path(f"{gcs_data_path}/{color}/{dataset_file}.parquet")
    gcs_block.upload_from_path(from_path=path, to_path=path)
    return


@flow()
def etl_web_to_gcs(
    color: str,
    year: int,
    month: int,
    local_data_path: str,
    gcs_data_path: str
) -> int:
    """The flow with ETL of getting taxi rides dataset and load it to GCS bucket"""
    dataset_file = f"{color}_tripdata_{year}-{month:02}"
    dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{dataset_file}.csv.gz"
    df = fetch(dataset_url)
    df_clean = clean(df)
    path = write_local(local_data_path, color, dataset_file, df_clean)
    write_gcs(gcs_data_path, color, dataset_file)
    return df_clean.shape[0]


@flow(log_prints=True)
def etl_web_to_gcs_base(
    color: str = "green",
    year: int = 2020,
    months: list[int] = [1],
    local_data_path: str = "data",
    gcs_data_path: str = "data"
):
    rows_processed = 0
    for month in months:
        rows_processed += etl_web_to_gcs(color, year, month, local_data_path, gcs_data_path)
    print(f"Flow cycle is finished, rows processed: {rows_processed}")


if __name__ == "__main__":
    etl_web_to_gcs_base()
