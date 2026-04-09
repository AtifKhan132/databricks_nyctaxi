# Databricks notebook source
from datetime import date, datetime, timezone
from dateutil.relativedelta import relativedelta
from pyspark.sql.functions import current_timestamp

# COMMAND ----------

#Obtain the year-month for 2 months prior to current month in yyy-MM format
two_months_ago = date.today() - relativedelta(months=2)
dates_to_process = two_months_ago.strftime("%Y-%m")

# COMMAND ----------

#Read all parquet files for the specified month from the landing directory into a dataframe
df = spark.read.format("parquet").load(f"/Volumes/nyc_taxi/00_landing/data_sources/nyctaxi_yellow/{dates_to_process}")

# COMMAND ----------

#Add a column to capture when the data was processed
df = df.withColumn("processed_timesatamp", current_timestamp())

# COMMAND ----------

df.display()

# COMMAND ----------

#Write the dataframe to Unity catalog managed delta table in the bronze schema, appending the new data
df.write.mode("append").saveAsTable("nyc_taxi.01_bronze.yellow_trips_raw")