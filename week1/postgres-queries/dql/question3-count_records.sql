---
--- Question 3. Count records 
---
--- How many taxi trips were totally made on January 15?
--- Tip: started and finished on 2019-01-15. 
select count(*)
from green_taxi_rides gtr 
where 
	lpep_pickup_datetime between '2019-01-15 00:00:00' and '2019-01-15 23:59:59'
	and lpep_dropoff_datetime between '2019-01-15 00:00:00' and '2019-01-15 23:59:59'