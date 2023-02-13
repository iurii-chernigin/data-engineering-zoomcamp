CREATE OR REPLACE TABLE zoomcamp.fhv_taxi_rides_partitioned_clustered
PARTITION BY DATE(pickup_datetime)
CLUSTER BY Affiliated_base_number
AS
SELECT *
FROM zoomcamp.fhv_taxi_rides;