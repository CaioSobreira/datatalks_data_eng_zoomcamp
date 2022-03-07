### DataTalks.club Data Engineering Zoomcamp

#### Week 4 homework answers

#

### Link to dbt Cloud repo: https://github.com/CaioSobreira/de_zoomcamp_dbt_bigquery

#### Question 1:

What is the count of records in the model fact_trips after running all models with the test run variable disabled and filtering for 2019 and 2020 data only (pickup datetime)

##### Answer : 61393150
##### SQL CODE (BigQuery)

	SELECT 
		count(1) 
	FROM 
		`spatial-lodge-339409.production.fact_trips` 
	WHERE 
		pickup_datetime BETWEEN '2019-01-01' AND '2020-12-31';
		
#### Question 2:

What is the distribution between service type filtering by years 2019 and 2020 data as done in the videos

##### Answer : Yellow 89.8% | Green 10.2%

#### Question 3:

What is the count of records in the model stg_fhv_tripdata after running all models with the test run variable disabled (:false)

##### Answer : 42084899

##### SQL CODE (BigQuery)

	SELECT 
		COUNT(1) 
	FROM 
		`spatial-lodge-339409.dbt_de_zc.stg_fhv_tripdata` 
	WHERE 
		EXTRACT(YEAR FROM pickup_datetime) = 2019;
		
#### Question 4:

What is the count of records in the model fact_fhv_trips after running all dependencies with the test run variable disabled (:false)

##### Answer : 22676253

##### SQL CODE (BigQuery)

	SELECT 
		count(1) 
	FROM 
		`spatial-lodge-339409.dbt_de_zc.fact_fhv_trips` 
	WHERE 
		EXTRACT(YEAR FROM pickup_datetime) = 2019;
		
#### Question 5:

What is the month with the biggest amount of rides after building a tile for the fact_fhv_trips table

##### Answer : january
