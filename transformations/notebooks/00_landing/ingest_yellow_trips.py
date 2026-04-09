# Databricks notebook source
from datetime import date, datetime, timezone
from dateutil.relativedelta import relativedelta
from modules.utils.project_root import get_project_root
from modules.data_loader.file_downloader import download_file
from modules.utils.date_utils import get_target_yyyymm

project_root = get_project_root()

# COMMAND ----------

#Obtain the year-month for 2 months prior to current month in yyy-MM format
dates_to_process = get_target_yyyymm(2)

dir_path = f"/Volumes/nyc_taxi/00_landing/data_sources/nyctaxi_yellow/{dates_to_process}"

local_path = f"{dir_path}/yellow_tripdata_{dates_to_process}.parquet"

try:
    #check if the file already exists
    dbutils.fs.ls(local_path)

    #set continue_downstream to no if the file already exists
    dbutils.jobs.taskValues.set(key="continue_downstream", value="no")
    print("File already exists. Skipping download and aborting downstream tasks")
except:
    try:
        url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{dates_to_process}.parquet"

        #download data files
        download_file(url, dir_path, local_path)

        #set continue_downstream to yes if the file was loaded successfully
        dbutils.jobs.taskValues.set(key="continue_downstream", value="yes")
        print("File successfully uploaded in current run")
    except Exception as e:
        #set continue_downstream to yes if the file was loaded successfully
        dbutils.jobs.taskValue.set(key="continue_downstream", value="no")
        print(f"File download failed. Skipping downstream tasks: {str(e)}")

dbutils.fs.ls("/Volumes/nyc_taxi/00_landing/data_sources/nyctaxi_yellow")