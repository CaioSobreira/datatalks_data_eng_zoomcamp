### DataTalks.club Data Engineering Zoomcamp

#### Week 3 homework answers

#

#### Question 1:

What is count for fhv vehicles data for year 2019

##### Answer : 42084899
##### SQL CODE (BigQuery)

	SELECT 
		count(1) 
	FROM 
		`spatial-lodge-339409.trips_data_all.external_table_fhv_2019`
		
#### Question 2:

How many distinct dispatching_base_num we have in fhv for 2019

##### Answer : 792
##### SQL CODE (BigQuery)

	SELECT 
		COUNT(DISTINCT(dispatching_base_num)) 
	FROM 
		`spatial-lodge-339409.trips_data_all.external_table_fhv_2019`


#### Question 3:

Best strategy to optimise if query always filter by dropoff_datetime and order by dispatching_base_num

##### Answer : Partition by dropoff_datetime and cluster by dispatching_base_num

		
#### Question 4:

What is the count, estimated and actual data processed for query which counts trip between 2019/01/01 and 2019/03/31 for dispatching_base_num B00987, B02060, B02279

##### Answer : COUNT = 26647 | ESTIMATED = 400,1| ACTUAL = 132,9 MB
##### SQL CODE

	--CREATING PARTITIONED AND CLUSTERED TABLE:
	CREATE OR REPLACE TABLE 
		spatial-lodge-339409.trips_data_all.table_fhv_2019_partit_clust
	PARTITION BY 
		DATE(pickup_datetime)
	CLUSTER BY 
		dispatching_base_num
	AS
	SELECT 
		dispatching_base_num, pickup_datetime, dropoff_datetime, PULocationID, DOLocationID, SR_Flag FROM `spatial-lodge-339409.trips_data_all.external_table_fhv_2019`;
	
	--QUERYING
	SELECT 
		COUNT(*)
	FROM
		`spatial-lodge-339409.trips_data_all.table_fhv_2019_partit_clust`
	WHERE 
		DATE(pickup_datetime) BETWEEN '2019-01-01' AND '2019-03-31'
	AND
		dispatching_base_num IN ('B00987', 'B02060', 'B02279');
		
		
#### Question 5:

What will be the best partitioning or clustering strategy when filtering on dispatching_base_num and SR_Flag

##### Answer : Cluster by dispatching_base_num and SR_Flag


#### Question 6:

What improvements can be seen by partitioning and clustering for data size less than 1 GB

##### Answer : No clustering nor partitioning for tables with data size less than 1 GB, because metadata processing time would exceed a non partitioned/clustered table processing time


#### Question 7:

In which format does BigQuery save data

##### Answer : Column-oriented format