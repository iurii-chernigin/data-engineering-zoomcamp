CREATE OR REPLACE TABLE zoomcamp.fhv_taxi_rides_partitioned
PARTITION BY DATE(pickup_datetime)
AS
SELECT *
FROM zoomcamp.fhv_taxi_rides;