# Databricks notebook source
import urllib.request
import os
import shutil

url = f"https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"

response = urllib.request.urlopen(url)

dir_path = f"/Volumes/nyc_taxi/00_landing/data_sources/lookup"

os.makedirs(dir_path, exist_ok=True)

local_path = f"{dir_path}/taxi_zone_lookup.csv"
  
with open(local_path, 'wb') as out_file:
    shutil.copyfileobj(response, out_file)
dbutils.fs.ls("/Volumes/nyc_taxi/00_landing/data_sources/lookup")

# COMMAND ----------

