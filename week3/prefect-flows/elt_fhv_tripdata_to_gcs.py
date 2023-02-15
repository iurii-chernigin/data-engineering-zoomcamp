from prefect import task, flow
from prefect_gcp.cloud_storage import GcsBucket
from pathlib import Path
import pandas as pd


@task(retries=3, retry_delay_seconds=[10, 30, 60], log_prints=True)
def fetch_by_url(url: str) -> pd.DataFrame:
    """ Download data by URL """
    print(f"Dataset's URL to download: {url}")
    df = pd.read_csv(url)
    print(f"Dataframe (with {df.shape[0]} rows) is downloaded from {url}")

    return df


@task(retries=3, retry_delay_seconds=[10, 30, 60], log_prints=True)
def save_to_local(df: pd.DataFrame, path: str, format: str) -> Path:
    """ Save downloaded dataset to local filesystem and return path"""
    print(f"Input parameters of save_to_local fn: format - {format}, path - {path}")
    if format == 'parquet':
        save_result = df.to_parquet(path, compression='gzip')
    elif format == 'csv.gz':
        save_result = df.to_csv(path, compression='gzip')

    print(f"Dataset is saved to local filisystem: {save_result}")

    return save_result


@task(retries=3, retry_delay_seconds=[10, 30, 60], log_prints=True)
def upload_to_gcs(dataset_local_path: str, dataset_gcs_path: str, gcs_block_name: str) -> bool:
    """ Upload files to the GCS bucket and return True if saving is success"""
    gcs_block = GcsBucket.load(gcs_block_name)
    out_path = gcs_block.upload_from_path(
        from_path=dataset_local_path,
        to_path=dataset_gcs_path)
    print("GCS uploading result: ", out_path)

    return True if out_path is not None else False


@flow(log_prints=True)
def elt_fhv_tripdata_to_gcs(
    year: int,
    month: int,
    saving_format: str,
    local_data_path: str,
    gcs_data_path: str,
    gcs_block_name: str
) -> None:
    """ Main flow that download one dataset and upload it to the GCS bucket """
    filename = f"fhv_tripdata_{year}-{month:02}"
    url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/{filename}.csv.gz"
    local_dataset_path = Path(f"{local_data_path}/{filename}.{saving_format}")
    gcs_dataset_path = Path(f"{gcs_data_path}/{filename}.{saving_format}")
    
    df = fetch_by_url(url)
    save_to_local(df, local_dataset_path, saving_format)
    gcs_uploading_state = upload_to_gcs(local_dataset_path, gcs_dataset_path, gcs_block_name)
    print("Flow finish with ", "success" if gcs_uploading_state is True else "failure")

    return 


@flow
def elt_fhv_tripdata_to_gcs_base(
    year: int = 2019,
    months: list[int] = [2],
    saving_format: str = 'parquet',
    local_data_path: str = "../prefect-data",
    gcs_data_path: str = "data/fhv",
    gcs_block_name: str = "gcs-bucket-prefect-flows"
) -> None:
    """Base flow for parametrized control of data loading"""
    for month in months:
        elt_fhv_tripdata_to_gcs(year, month, saving_format, local_data_path, gcs_data_path, gcs_block_name)


if __name__ == "__main__":
    elt_fhv_tripdata_to_gcs_base()