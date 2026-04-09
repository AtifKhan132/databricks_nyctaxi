# Databricks notebook source
from pyspark.sql.functions import current_timestamp, lit, col
from pyspark.sql.types import TimestampType, IntegerType

# COMMAND ----------

df = spark.read.format('csv').option('header','true').load('/Volumes/nyc_taxi/00_landing/data_sources/lookup/taxi_zone_lookup.csv')

# COMMAND ----------

df = df.select(
                col('LocationID').cast(IntegerType()).alias('location_id'),
                col('Borough').alias('borough'),
                col('Zone').alias('zone'),
                col('service_zone'),
                current_timestamp().alias('effective_date'),
                lit(None).cast(TimestampType()).alias('end_date')
            )

# COMMAND ----------

df.write.mode('overwrite').saveAsTable('nyc_taxi.02_silver.taxi_zone_lookup')