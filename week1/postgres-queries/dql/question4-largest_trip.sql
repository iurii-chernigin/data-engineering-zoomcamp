---
--- Question 4. Largest trip for each day
---
--- Which was the day with the largest trip distance
--- Use the pick up time for your calculations.
select lpep_pickup_datetime::date as largest_trip_date
from green_taxi_rides
where trip_distance in (
	select max(trip_distance) as max_trip_distance
	from green_taxi_rides
)