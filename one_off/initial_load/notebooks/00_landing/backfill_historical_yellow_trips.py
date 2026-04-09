# Databricks notebook source
import urllib.request
import shutil
import os

# COMMAND ----------

dates_to_process = ['2025-08','2025-09', '2025-10', '2025-11', '2025-12', '2026-01']

for date in dates_to_process:
  url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{date}.parquet"

  response = urllib.request.urlopen(url)

  dir_path = f"/Volumes/nyc_taxi/00_landing/data_sources/nyctaxi_yellow/{date}"

  os.makedirs(dir_path, exist_ok=True)

  local_path = f"{dir_path}/yellow_tripdata_{date}.parquet"
  
  with open(local_path, 'wb') as out_file:
    shutil.copyfileobj(response, out_file)
dbutils.fs.ls("/Volumes/nyc_taxi/00_landing/data_sources/nyctaxi_yellow")

# COMMAND ----------

