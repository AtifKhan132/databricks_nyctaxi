# Databricks notebook source
# import urllib.request
# import os
# import shutil
# import sys
from modules.data_loader.file_downloader import download_file
from modules.utils.project_root import get_project_root

project_root = get_project_root()

try:
  url = f"https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"

  dir_path = f"/Volumes/nyc_taxi/00_landing/data_sources/lookup"

  local_path = f"{dir_path}/taxi_zone_lookup.csv"

  download_file(url, dir_path, local_path)
  
  dbutils.jobs.taskValues.set(key="continue_downstream", value="yes")
  print("File successfully uploaded")

except Exception as e:
  dbutils.jobs.taskValues.set(key="continue_downstream", value="no")
  print(f"File download failed: {str(e)}")  
dbutils.fs.ls("/Volumes/nyc_taxi/00_landing/data_sources/lookup")