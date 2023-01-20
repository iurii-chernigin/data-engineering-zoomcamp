---
--- Question 5. The number of passengers
---
--- In 2019-01-01 how many trips had 2 and 3 passengers?
select count(*) as trips, passenger_count 
from green_taxi_rides gtr 
where 
	lpep_pickup_datetime between '2019-01-01 00:00:00' and '2019-01-01 23:59:59'
	and passenger_count in (2, 3)
group by passenger_count