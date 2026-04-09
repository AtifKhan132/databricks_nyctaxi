# Databricks notebook source
from pyspark.sql.functions import count, max, min, avg, sum, round
from modules.utils.date_utils import get_months_start_n_months_ago

# COMMAND ----------

two_months_ago_start = get_months_start_n_months_ago(2)

# COMMAND ----------

df = spark.read.table("nyc_taxi.02_silver.yellow_trips_enriched").filter(f"tpep_pickup_datetime > '{two_months_ago_start}'")

# COMMAND ----------

df.display()

# COMMAND ----------

df = df.groupBy(df.tpep_pickup_datetime.cast("date").alias("pickup_date")).\
    agg(
        count("*").alias("total_trips"),
        round(avg("passenger_count"),1).alias("average_passengers"),
        round(avg("trip_distance"),1).alias("average_distance"),
        round(avg("fare_amount"),1).alias("average_fare_per_trip"),
        max("fare_amount").alias("max_fare"),
        min("fare_amount").alias("min_fare"),
        round(sum("total_amount"),1).alias("total_revenue")
    )

# COMMAND ----------

df.write.mode("append").saveAsTable("nyc_taxi.03_gold.daily_trip_summary")
