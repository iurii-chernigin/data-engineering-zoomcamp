CREATE OR REPLACE EXTERNAL TABLE zoomcamp.fhv_taxi_rides_external
OPTIONS (
  format = "CSV",
  uris = ["gs://prefect-flows-data/data/fhv/fhv_tripdata_2019-*.csv.gz"] --- Prefect prepared data
)