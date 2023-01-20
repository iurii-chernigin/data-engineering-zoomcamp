---
--- Question 6. Largest tip
---
--- For the passengers picked up in the Astoria Zone which was the drop up zone that had the largest tip?
--- We want the name of the zone, not the id.
with taxi_rides as (
	select 
		max(tip_amount) as max_tip_amount,
		dropoff_location_id
	from green_taxi_rides gtr 
	where pickup_location_id in (
			select location_id
			from taxi_zones_lookup tzl 
			where zone = 'Astoria'
		)
	group by dropoff_location_id
)
select 
	max_tip_amount as largest_tip, 
	zone
from taxi_rides tr
left join taxi_zones_lookup tzl
	on tr.dropoff_location_id = tzl.location_id 
where max_tip_amount in (
		select max(max_tip_amount) as largest_tip
		from taxi_rides
	)