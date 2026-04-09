# Databricks notebook source
from pyspark.sql.functions import current_timestamp
from modules.utils.date_utils import get_target_yyyymm
from modules.transformations.metadata import current_timestamp

# COMMAND ----------

#Obtain the year-month for 2 months prior to current month in yyy-MM format
dates_to_process = get_target_yyyymm(2)

# COMMAND ----------

#Read all parquet files for the specified month from the landing directory into a dataframe
df = spark.read.format("parquet").load(f"/Volumes/nyc_taxi/00_landing/data_sources/nyctaxi_yellow/{dates_to_process}")

# COMMAND ----------

#Add a column to capture when the data was processed
df = current_timestamp(df)

# COMMAND ----------

df.display()

# COMMAND ----------

#Write the dataframe to Unity catalog managed delta table in the bronze schema, appending the new data
df.write.mode("append").saveAsTable("nyc_taxi.01_bronze.yellow_trips_raw")