### DataTalks.club Data Engineering Zoomcamp

#### Week 1 homework - Answers to SQL queries questions

#

#### Question 3. Count records

How many taxi trips were there on January 15?

Consider only trips that started on January 15.

##### Answer : 53024
##### SQL CODE

	SELECT 
		tpep_pickup_datetime::DATE AS pickup_date, 
		COUNT(1) trips_qty
	FROM 
		public.yellow_taxi_trips
	WHERE
		tpep_pickup_datetime::DATE = '2021-01-15'
	GROUP BY 
		pickup_date;
		
#### Question 4. Largest tip for each day

Find the largest tip for each day. On which day it was the largest tip in January?

Use the pick up time for your calculations.

(note: it's not a typo, it's "tip", not "trip")

##### Answer : 2021-01-20 | 1140.44
##### SQL CODE

	SELECT 
		tpep_pickup_datetime::DATE AS pickup_date, 
		MAX(tip_amount) AS max_tip_amount
	FROM 
		public.yellow_taxi_trips
	GROUP BY 
		pickup_date
	ORDER BY 
		max_tip_amount DESC;

#### Question 5. Most popular destination

What was the most popular destination for passengers picked up in central park on January 14?

Use the pick up time for your calculations.

Enter the zone name (not id). If the zone name is unknown (missing), write "Unknown"

##### Answer : Upper East Side South | 97
##### SQL CODE

	SELECT 
		pickup.zone AS pickup_zone,
		COALESCE(NULLIF(NULLIF(dropoff.zone, 'NA'), 'NV'), 'Unknown') AS dropoff_zone,  
		COUNT(1) AS trips_qty
	FROM 
		public.yellow_taxi_trips ytt
	LEFT JOIN
		public.taxi_zone_lookup pickup
	ON
		ytt."PULocationID" = pickup."LocationID"
	LEFT JOIN
		public.taxi_zone_lookup dropoff
	ON
		ytt."DOLocationID" = dropoff."LocationID"
	WHERE
		pickup."LocationID" = 43
	AND 
		tpep_pickup_datetime::DATE = '2021-01-14'	
	GROUP BY
		pickup_zone,
		dropoff_zone
	ORDER BY 
		trips_qty DESC;
		
#### Question 6. Most expensive locations

What's the pickup-dropoff pair with the largest average price for a ride (calculated based on total_amount)?

Enter two zone names separated by a slash

For example:

"Jamaica Bay / Clinton East"

If any of the zone names are unknown (missing), write "Unknown". For example, "Unknown / Clinton East".

##### Answer : Alphabet City / Unknown | 490.54
##### SQL CODE

	SELECT 
		COALESCE(NULLIF(NULLIF(pickup.zone, 'NA'), 'NV'), 'Unknown') || ' / ' || COALESCE(NULLIF(NULLIF(dropoff.zone, 'NA'), 'NV'), 'Unknown') AS trip_itinerary,  
		ROUND(avg(total_amount::numeric), 2) AS avg_total_amount
	FROM 
		public.yellow_taxi_trips ytt
	LEFT JOIN
		public.taxi_zone_lookup pickup
	ON
		ytt."PULocationID" = pickup."LocationID"
	LEFT JOIN
		public.taxi_zone_lookup dropoff
	ON
		ytt."DOLocationID" = dropoff."LocationID"
	GROUP BY
		trip_itinerary
	ORDER BY 
		avg_total_amount DESC;
