# Databricks notebook source
spark.sql("create catalog if not exists nyc_taxi managed location 'abfss://unity-catalog-storage@dbstoragexsgz7inqadb7y.dfs.core.windows.net/7405608256380143'")

# COMMAND ----------

spark.sql("create schema if not exists nyc_taxi.00_landing")
spark.sql("create schema if not exists nyc_taxi.01_bronze")
spark.sql("create schema if not exists nyc_taxi.02_silver")
spark.sql("create schema if not exists nyc_taxi.03_gold")

# COMMAND ----------

spark.sql("create volume if not exists nyc_taxi.00_landing.data_sources")