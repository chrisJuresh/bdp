import sys, string
import os
import socket
import time
import operator
import boto3
import json

from datetime import datetime
from graphframes import *

from pyspark.sql import SparkSession
from pyspark.sql.types import BooleanType, DateType
from pyspark.sql.functions import max, unix_timestamp, desc, date_format, year, month, avg, when, to_date, count, col, udf, dayofweek, expr


def fieldcleansing(dataframe):

  if (dataframe.first()['taxi_type']=='yellow_taxi'):
    columns = dataframe.columns
    dataframe = dataframe.select(*(col(c).cast("string").alias(c) for c in columns))
    dataframe = dataframe.filter(unix_timestamp(dataframe['tpep_pickup_datetime'],'yyyy-MM-dd HH:mm:ss').isNotNull())
    dataframe = dataframe.filter(unix_timestamp(dataframe['tpep_dropoff_datetime'],'yyyy-MM-dd HH:mm:ss').isNotNull())
    
    
  elif (dataframe.first()['taxi_type']=='green_taxi'):
    columns = dataframe.columns
    dataframe = dataframe.select(*(col(c).cast("string").alias(c) for c in columns))
    dataframe = dataframe.filter(unix_timestamp(dataframe['lpep_pickup_datetime'],'yyyy-MM-dd HH:mm:ss').isNotNull())
    dataframe = dataframe.filter(unix_timestamp(dataframe['lpep_dropoff_datetime'],'yyyy-MM-dd HH:mm:ss').isNotNull())
  
  
  dataframe = dataframe.filter((dataframe["trip_distance"] >= 0) & (dataframe["fare_amount"] >= 0))
  return dataframe 
  
    

if __name__ == "__main__":

    spark = SparkSession\
        .builder\
        .appName("TestDataset")\
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY")\
        .getOrCreate()
    
    # shared read-only object bucket containing datasets
    s3_data_repository_bucket = os.environ['DATA_REPOSITORY_BUCKET']

    s3_endpoint_url = os.environ['S3_ENDPOINT_URL']+':'+os.environ['BUCKET_PORT']
    s3_access_key_id = os.environ['AWS_ACCESS_KEY_ID']
    s3_secret_access_key = os.environ['AWS_SECRET_ACCESS_KEY']
    s3_bucket = os.environ['BUCKET_NAME']

    hadoopConf = spark.sparkContext._jsc.hadoopConfiguration()
    hadoopConf.set("fs.s3a.endpoint", s3_endpoint_url)
    hadoopConf.set("fs.s3a.access.key", s3_access_key_id)
    hadoopConf.set("fs.s3a.secret.key", s3_secret_access_key)
    hadoopConf.set("fs.s3a.path.style.access", "true")
    hadoopConf.set("fs.s3a.connection.ssl.enabled", "false")

    yellow_tripdata_df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("s3a://" + s3_data_repository_bucket + "/ECS765/nyc_taxi/yellow_tripdata/2023/*")
    green_tripdata_df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("s3a://" + s3_data_repository_bucket + "/ECS765/nyc_taxi/green_tripdata/2023/*")
    taxi_zone_lookup = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("s3a://" + s3_data_repository_bucket + "/ECS765/nyc_taxi/taxi_zone_lookup.csv")


    # checking and removing any null values or wrong format in the dataset and cleaning them for further processing
    yellow_tripdata_df = fieldcleansing(yellow_tripdata_df)
    green_tripdata_df = fieldcleansing(green_tripdata_df)

####################################################################################################################

    # Common
    now = datetime.now()
    date_time = now.strftime("%d-%m-%Y_%H:%M:%S")
    
    delimiter = "=" * 1000

    
    # Task 1: Merging Datasets   
    join_columns = ["PULocationID", "DOLocationID"]
    yellow_taxi_zone_df = yellow_tripdata_df.join(taxi_zone_lookup, yellow_tripdata_df.PULocationID == taxi_zone_lookup.LocationID, 'left').withColumnRenamed("Borough", "Pickup_Borough").withColumnRenamed("Zone", "Pickup_Zone").withColumnRenamed("service_zone", "Pickup_service_zone").drop("LocationID")
    yellow_taxi_zone_df = yellow_taxi_zone_df.join(taxi_zone_lookup, yellow_taxi_zone_df.DOLocationID == taxi_zone_lookup.LocationID, 'left').withColumnRenamed("Borough", "Dropoff_Borough").withColumnRenamed("Zone", "Dropoff_Zone").withColumnRenamed("service_zone", "Dropoff_service_zone").drop("LocationID")

    green_taxi_zone_df = green_tripdata_df.join(taxi_zone_lookup, green_tripdata_df.PULocationID == taxi_zone_lookup.LocationID, 'left').withColumnRenamed("Borough", "Pickup_Borough").withColumnRenamed("Zone", "Pickup_Zone").withColumnRenamed("service_zone", "Pickup_service_zone").drop("LocationID")
    green_taxi_zone_df = green_taxi_zone_df.join(taxi_zone_lookup, green_tripdata_df.DOLocationID == taxi_zone_lookup.LocationID, 'left').withColumnRenamed("Borough", "Dropoff_Borough").withColumnRenamed("Zone", "Dropoff_Zone").withColumnRenamed("service_zone", "Dropoff_service_zone").drop("LocationID")

    print(delimiter)
    yellow_taxi_zone_df.printSchema()
    green_taxi_zone_df.printSchema()
    print(delimiter)
    print("Yellow Taxi Zone DataFrame - Rows:", yellow_taxi_zone_df.count(), "Columns:", len(yellow_taxi_zone_df.columns))
    print(delimiter)
    print("Green Taxi Zone DataFrame - Rows:", green_taxi_zone_df.count(), "Columns:", len(green_taxi_zone_df.columns))
    print(delimiter)
    yellow_taxi_zone_df.show()
    green_taxi_zone_df.show()
    print(delimiter)

    
    # Task 2: Aggregation of Data
    yellow_pickup_counts = yellow_taxi_zone_df.groupBy('Pickup_Borough').count().collect()
    green_pickup_counts = green_taxi_zone_df.groupBy('Pickup_Borough').count().collect()
    yellow_dropoff_counts = yellow_taxi_zone_df.groupBy('Dropoff_Borough').count().collect()
    green_dropoff_counts = green_taxi_zone_df.groupBy('Dropoff_Borough').count().collect()

    
    # Task 3: Filtering Data
    def filter_condition(fare, distance, pickup_datetime):
        fare = float(fare)
        distance = float(distance)
        pickup_date = datetime.strptime(pickup_datetime, "%Y-%m-%d %H:%M:%S").date()
        return fare > 50 and distance < 1 and pickup_date >= datetime(2023, 1, 1).date() and pickup_date <= datetime(2023, 1, 7).date()

    filter_udf = udf(filter_condition, BooleanType())

    yellow_filtered_df = yellow_tripdata_df.filter(filter_udf(yellow_tripdata_df['fare_amount'], yellow_tripdata_df['trip_distance'], yellow_tripdata_df['tpep_pickup_datetime']))

    yellow_filtered_df = yellow_filtered_df.withColumn("pickup_date", to_date("tpep_pickup_datetime", "yyyy-MM-dd"))
    yellow_filtered_df = yellow_filtered_df.withColumn("day_of_week", dayofweek("pickup_date"))

    daywise_trip_counts = yellow_filtered_df.groupBy("day_of_week").count().collect()

    
    # Task 4: Top-K Processing
    yellow_pickup_top5 = yellow_taxi_zone_df.groupBy('Pickup_Borough').count().orderBy(desc('count')).limit(5)
    yellow_dropoff_top5 = yellow_taxi_zone_df.groupBy('Dropoff_Borough').count().orderBy(desc('count')).limit(5)
    
    green_pickup_top5 = green_taxi_zone_df.groupBy('Pickup_Borough').count().orderBy(desc('count')).limit(5)
    green_dropoff_top5 = green_taxi_zone_df.groupBy('Dropoff_Borough').count().orderBy(desc('count')).limit(5)

    # Task 5: Finding anomalies
    yellow_june_2023_df = yellow_taxi_zone_df.filter(
        (year(col("tpep_pickup_datetime")) == 2023) & 
        (month(col("tpep_pickup_datetime")) == 6)
    )
    
    daily_trips_june = yellow_june_2023_df.groupBy(date_format(col("tpep_pickup_datetime"), "yyyy-MM-dd").alias("date")).count()

    stats = daily_trips_june.approxQuantile("count", [0.4, 0.6], 0)
    Q1, Q3 = stats
    IQR = Q3 - Q1

    lower_bound = Q1 - 1.5 * IQR
    upper_bound = Q3 + 1.5 * IQR

    daily_trips_june = daily_trips_june.withColumn(
        "anomaly",
        when(col("count") > upper_bound, "High")
        .when(col("count") < lower_bound, "Low")
        .otherwise("Normal")
    )

    daily_trips_plot_data_june = daily_trips_june.select("date", "count", "anomaly").orderBy("date").collect()
    

    # Task 6: Statistical Processing
    yellow_march_2023_df = yellow_taxi_zone_df.filter(
        (year(col("tpep_pickup_datetime")) == 2023) & 
        (month(col("tpep_pickup_datetime")) == 3)
    )

    yellow_march_2023_df = yellow_march_2023_df.filter(col("trip_distance") > 0)

    yellow_march_2023_df = yellow_march_2023_df.withColumn("fare_per_mile", col("fare_amount") / col("trip_distance"))
    
    average_fare_per_mile = yellow_march_2023_df.agg(avg("fare_per_mile").alias("avg_fare_per_mile")).collect()[0]["avg_fare_per_mile"]

    fare_per_mile_data = yellow_march_2023_df.select("fare_per_mile").collect()


    # Task 7: Data Distribution
    total_yellow_trips = yellow_tripdata_df.count()
    solo_yellow_trips = yellow_tripdata_df.filter(col("passenger_count") == 1).count()
    percentage_solo_yellow = (solo_yellow_trips / total_yellow_trips) * 100

    total_green_trips = green_tripdata_df.count()
    solo_green_trips = green_tripdata_df.filter(col("passenger_count") == 1).count()
    percentage_solo_green = (solo_green_trips / total_green_trips) * 100

    print(delimiter)
    print(f"Percentage of solo trips in Yellow Taxi data: {percentage_solo_yellow:.2f}%")
    print(f"Percentage of solo trips in Green Taxi data: {percentage_solo_green:.2f}%")
    print(delimiter)

    
    # Task 10: Time-window based Analysis
    def most_popular_month(dataframe, pickup_datetime_column, taxi_type):
        monthly_trips = dataframe.groupBy(month(col(pickup_datetime_column)).alias("month")).count()
        most_trips_month = monthly_trips.orderBy(desc("count")).first()
        return {"taxi_type": taxi_type, "month": most_trips_month["month"], "trips": most_trips_month["count"]}

    most_popular_month_yellow = most_popular_month(yellow_tripdata_df, "tpep_pickup_datetime", "yellow_taxi")
    most_popular_month_green = most_popular_month(green_tripdata_df, "lpep_pickup_datetime", "green_taxi")

    
    #File Output
    my_bucket_resource = boto3.resource('s3',
                                        endpoint_url='http://' + s3_endpoint_url,
                                        aws_access_key_id=s3_access_key_id,
                                        aws_secret_access_key=s3_secret_access_key)
    def upload_to_s3(file_name, data):
        result_file_name = f'taxi_data_{date_time}/{file_name}.json'
        my_result_object = my_bucket_resource.Object(s3_bucket, result_file_name)
        my_result_object.put(Body=data)
    #Task2
    yellow_pickup_json = json.dumps([row.asDict() for row in yellow_pickup_counts])
    green_pickup_json = json.dumps([row.asDict() for row in green_pickup_counts])
    yellow_dropoff_json = json.dumps([row.asDict() for row in yellow_dropoff_counts])
    green_dropoff_json = json.dumps([row.asDict() for row in green_dropoff_counts])
    upload_to_s3('yellow_pickup', yellow_pickup_json)
    upload_to_s3('green_pickup', green_pickup_json)
    upload_to_s3('yellow_dropoff', yellow_dropoff_json)
    upload_to_s3('green_dropoff', green_dropoff_json)
    #Task3
    daywise_trip_counts_json = json.dumps([row.asDict() for row in daywise_trip_counts])
    upload_to_s3('yellow_filtered_trip_counts', daywise_trip_counts_json)
    #Task4
    yellow_pickup_top5_json = json.dumps([row.asDict() for row in yellow_pickup_top5.collect()])
    yellow_dropoff_top5_json = json.dumps([row.asDict() for row in yellow_dropoff_top5.collect()])
    green_pickup_top5_json = json.dumps([row.asDict() for row in green_pickup_top5.collect()])
    green_dropoff_top5_json = json.dumps([row.asDict() for row in green_dropoff_top5.collect()])
    upload_to_s3('yellow_pickup_top5', yellow_pickup_top5_json)
    upload_to_s3('yellow_dropoff_top5', yellow_dropoff_top5_json)
    upload_to_s3('green_pickup_top5', green_pickup_top5_json)
    upload_to_s3('green_dropoff_top5', green_dropoff_top5_json)
    #Task5
    daily_trips_plot_json_june = json.dumps([row.asDict() for row in daily_trips_plot_data_june])
    upload_to_s3('yellow_june_2023_anomalies', daily_trips_plot_json_june)
    #Task6
    fare_per_mile_plot_json = json.dumps([{"fare_per_mile": row["fare_per_mile"], "avg_fare_per_mile": average_fare_per_mile} for row in fare_per_mile_data])
    upload_to_s3('yellow_march_2023_fare_per_mile', fare_per_mile_plot_json)
    #Task10
    most_popular_months_json = json.dumps([most_popular_month_yellow, most_popular_month_green])
    upload_to_s3('most_popular_months', most_popular_months_json)
   
    spark.stop()
    
