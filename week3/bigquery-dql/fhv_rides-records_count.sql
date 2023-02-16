--- What is the count for fhv vehicle records for year 2019?
SELECT COUNT(*) as total_records
FROM zoomcamp.fhv_taxi_rides;

--- How many records have both a blank (null) PUlocationID and DOlocationID in the entire dataset?
SELECT COUNT(*) total_records
FROM zoomcamp.fhv_taxi_rides
WHERE PUlocationID IS NULL AND DOlocationID IS NULL;


--- Write a query to retrieve the distinct affiliated_base_number 
--- between pickup_datetime 2019/03/01 and 2019/03/31 (inclusive)
SELECT DISTINCT Affiliated_base_number
FROM zoomcamp.fhv_taxi_rides_partitioned_clustered
WHERE DATE(pickup_datetime) BETWEEN '2019-03-01' AND '2019-03-31'
ORDER BY Affiliated_base_number;