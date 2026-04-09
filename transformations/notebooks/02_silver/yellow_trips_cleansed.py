# Databricks notebook source
from pyspark.sql.functions import col, when, timestamp_diff
from datetime import date
from dateutil.relativedelta import relativedelta

# COMMAND ----------

two_months_ago_start = date.today().replace(day=1) - relativedelta(months=2)

one_month_ago_start = date.today().replace(day=1) - relativedelta(months=1)

# COMMAND ----------

df = spark.read.table("nyc_taxi.01_bronze.yellow_trips_raw")

# COMMAND ----------

display(df)

# COMMAND ----------

df = df.filter(f"tpep_pickup_datetime >= '{two_months_ago_start}' AND tpep_pickup_datetime < '{one_month_ago_start}'")

# COMMAND ----------

df = df.select(
    when(col("VendorID") == 1, "Creative Mobile Technologies, LLC").
        when(col("VendorID") == 2, "Curb Mobility, LLC").
        when(col("VendorID") ==6, "Myle Technologies Inc").
        when(col("VendorID") == 8, "Helix").
        otherwise("Unknown").
        alias("vendor"),

    col("tpep_pickup_datetime"),
    col("tpep_dropoff_datetime"),
    timestamp_diff('MINUTE', df.tpep_pickup_datetime, df.tpep_dropoff_datetime).alias("trip_duration"),
    col("passenger_count"),
    col("trip_distance"),

    when(col("RatecodeID") == 1, "Standard rate").
        when(col("RatecodeID") == 2, "JFK").
        when(col("RatecodeID") == 3, "Newark").
        when(col("RatecodeID") == 4, "Nassau or Westchester").
        when(col("RatecodeID") == 5, "Negotiated fare").
        when(col("RatecodeID") == 6, "Group ride").
        otherwise("Unknown").
        alias("rate_type"),
    
    col("store_and_fwd_flag"),
    col("PULocationID").alias("pu_lication_id"),
    col("DOLocationID").alias("do_lication_id"),

    when(col("payment_type") == 0, "Flex Fare trip").
        when(col("payment_type") == 1, "Credit card").
        when(col("payment_type") == 2, "Cash").
        when(col("payment_type") == 3, "No charge").
        when(col("payment_type") == 4, "Dispute").
        when(col("payment_type") == 6, "Voided trip").
        otherwise("Unknown").
        alias("payment_type"),
    col("fare_amount"),
    col("extra"),
    col("mta_tax"),
    col("tip_amount"),
    col("tolls_amount"),
    col("improvement_surcharge"),
    col("total_amount"),
    col("congestion_surcharge"),
    col("Airport_fee").alias("airport_fee"),
    col("cbd_congestion_fee").alias("cbd_congestion_fee"),
    col("processed_timesatamp")    
)

# COMMAND ----------

df.write.mode("append").saveAsTable("nyc_taxi.02_silver.yellow_trips_cleansed")