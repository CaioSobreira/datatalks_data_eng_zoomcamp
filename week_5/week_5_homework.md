### DataTalks.club Data Engineering Zoomcamp

#### Week 5 homework answers

#

#### Question 1. Install Spark and PySpark

Install Spark
Run PySpark
Create a local spark session
Execute spark.version

What's the output?

##### Answer : 
    scala> spark.version
    res0: String = 3.0.3
		
#### Question 2. HVFHW February 2021

Download the HVFHV data for february 2021:

    wget https://nyc-tlc.s3.amazonaws.com/trip+data/fhvhv_tripdata_2021-02.csv

Read it with Spark using the same schema as we did in the lessons. We will use this dataset for all the remaining questions.

Repartition it to 24 partitions and save it to parquet.

What's the size of the folder with results (in MB)?

##### Answer : 207.6 MB

#### Question 3. Count records

How many taxi trips were there on February 15?

Consider only trips that started on February 15.

##### Answer : 367170

##### Python code

    df_q3 = df \
       .withColumn('pickup_date', F.to_date(df.pickup_datetime)) \
       .withColumn('dropoff_date', F.to_date(df.pickup_datetime)) \
       .select('pickup_date', 'dropoff_date', 'PULocationID', 'DOLocationID')
    
    df_q3 \
        .filter(df_q3.pickup_date == '2021-02-15') \
        .count()
		
#### Question 4. Longest trip for each day

Now calculate the duration for each trip.

Trip starting on which day was the longest?

##### Answer : 2021-02-11 (1259 minutes)

##### Python code

	df_q4 = df \
	    .withColumn('pickup_date', F.to_date(df.pickup_datetime)) \
	    .withColumn("diff_seconds", df.dropoff_datetime.cast(types.LongType()) - 
		df.pickup_datetime.cast(types.LongType()))

	df_q4.registerTempTable('df_q4')

	spark.sql("""
	SELECT
	    pickup_date,
	    ROUND((max_duration / 60), 2) AS max_duration_minutes
	FROM
	    (SELECT
		pickup_date,
		MAX(diff_seconds) AS max_duration
	    FROM
		df_q4
	    GROUP BY
		pickup_date) q
	ORDER BY
	    max_duration DESC
	""").show()
		
#### Question 5. Most frequent dispatching_base_num

Now find the most frequently occurring dispatching_base_num in this dataset.

How many stages this spark job has?

Note: the answer may depend on how you write the query, so there are multiple correct answers. Select the one you have.

##### Answer : 2 stages

##### Python code

	df_q5 = df 

	df_q5.registerTempTable('df_q5')

	spark.sql("""
	SELECT
	    dispatching_base_num,
	    COUNT(1) AS qty
	FROM
	    df_q5
	GROUP BY
	    dispatching_base_num
	ORDER BY
	    qty DESC
	""").show()
	
#### Question 6. Most common locations pair

Find the most common pickup-dropoff pair.

For example:

"Jamaica Bay / Clinton East"

Enter two zone names separated by a slash

If any of the zone names are unknown (missing), use "Unknown". For example, "Unknown / Clinton East".

##### Answer : East New York / East New York : 45041

##### Python code

	!wget https://s3.amazonaws.com/nyc-tlc/misc/taxi+_zone_lookup.csv
	
	schema_lookup = types.StructType([
	    types.StructField('LocationID', types.IntegerType(), True),
	    types.StructField('Borough', types.StringType(), True),
	    types.StructField('Zone', types.StringType(), True),
	    types.StructField('service_zone', types.StringType(), True)
	])
	
	df_lookup = spark.read \
	    .option("header", "true") \
	    .schema(schema_lookup) \
	    .csv('taxi+_zone_lookup.csv')

	df_lookup.registerTempTable('df_lookup')
	
	df_q6 = df \
	   .select('PULocationID', 'DOLocationID')
   
	df_q6.registerTempTable('df_q6')	
	
	spark.sql("""
	SELECT
	    pu.LocationID,
	    do.LocationID,
	    COALESCE(NULLIF(NULLIF(pu.Zone, 'NA'), 'NV'), 'Unknown') || ' / ' || COALESCE(NULLIF(NULLIF(do.Zone, 'NA'), 'NV'), 'Unknown') AS trip_itinerary,
	    COUNT(1) AS qty
	FROM
	    df_q6 q6
	INNER JOIN
	    df_lookup pu
	ON
	    q6.PULocationID = pu.LocationID
	INNER JOIN
	    df_lookup do
	ON
	    q6.DOLocationID = do.LocationID
	GROUP BY
	    pu.LocationID,
	    do.LocationID,
	    trip_itinerary
	ORDER BY
	    qty DESC
	""").show()