from prefect import task, flow
from prefect_gcp.cloud_storage import GcsBucket
import wget 


@task
def fetch_by_url(url: str, saving_path: str) -> None:
    wget.download(url, saving_path)
    return


@task(log_prints=True)
def upload_to_gcs(dataset_local_path: str, dataset_gcs_path: str, gcs_block_name: str) -> bool:
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
    local_data_path: str,
    gcs_data_path: str,
    gcs_block_name: str
) -> None:
    filename = f"fhv_tripdata_{year}-{month:02}.csv.gz"
    url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/{filename}"
    local_dataset_path = f"{local_data_path}/{filename}"
    gcs_dataset_path = f"{gcs_data_path}/{filename}"
    
    fetch_by_url(url, local_dataset_path)
    gcs_uploading_state = upload_to_gcs(local_dataset_path, gcs_dataset_path, gcs_block_name)
    print("Flow finish with ", "success" if gcs_uploading_state is True else "failure")
    return 


@flow
def elt_fhv_tripdata_to_gcs_base(
    year: int = 2019,
    months: list[int] = [1],
    local_data_path: str = "../prefect-data",
    gcs_data_path: str = "data/fhv",
    gcs_block_name: str = "gcs-bucket-prefect-flows"
) -> None:
    """Base flow for parametrized control of data loading"""
    for month in months:
        elt_fhv_tripdata_to_gcs(year, month, local_data_path, gcs_data_path, gcs_block_name)


if __name__ == "__main__":
    elt_fhv_tripdata_to_gcs_base()