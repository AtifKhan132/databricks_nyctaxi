# Databricks notebook source
from datetime import datetime
from delta.tables import DeltaTable
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

# Fixed point-in-time used to "close" any changed activ records
#Using python timestamp ensures the exact same valueis written and can be reference if needed
end_timpestamp = datetime.now()

#load the SCD2 Delta Table
dt = DeltaTable.forName(spark, "nyc_taxi.02_silver.taxi_zone_lookup")

# COMMAND ----------

# ------------------------------------
# PASS 1: Close any active rows whoe tracked attributes have changed
# ------------------------------------

dt.alias("t").\
    merge(
        source = df.alias("s"),
        condition = "t.location_id = s.location_id AND t.end_date IS NULL AND (t.borough != s.borough OR t.zone != s.zone OR t.service_zone != s.service_zone)"
    ).\
    whenMatchedUpdate(
        set = {
            "end_date": lit(end_timpestamp)
        }
    ).\
    execute()

# COMMAND ----------

# -----------------------------
# PASS 2: Insert new current version
# ----------------------------
# Now insert a row for:
#   (a) keys we just closed in PASS 1 (no longer an active match), and 
#   (b) new keys that don't exist in the target table
# We again match on *activ* rows; anything without an active match is inserted.

# get the list of IDs that have been closed
insert_id_list = [row.location_id for row in dt.toDF().filter(f"end_date = '{end_timpestamp}'").select('location_id').collect()]

# If the list is empty don't try to add anything
if len(insert_id_list) == 0:
    print('No new records to insert')
else:
    dt.alias("t").\
        merge(
            source = df.alias("s"),
            condition = f"s.location_id not in ({','.join(map(str, insert_id_list))})"
        ).\
        whenNotMatchedInsert(
            values = {
                "location_id": col('s.location_id'),
                "borough": col('s.borough'),
                "zone": col('s.zone'),
                "service_zone": col('s.service_zone'),
                "effective_date": col('s.effective_date'),
                "end_date": lit(None).cast(TimestampType())}
        ).\
        execute()


# COMMAND ----------

# -----------------------------
# PASS 3: Insert brand-new keys (no historical row in target)
# ----------------------------

dt.alias("t").\
    merge(
        source = df.alias("s"),
        condition = "t.location_id = s.location_id AND t.end_date IS NULL"
    ).\
    whenNotMatchedInsert(
        values = {
            "location_id": col('s.location_id'),
            "borough": col('s.borough'),
            "zone": col('s.zone'),
            "service_zone": col('s.service_zone'),
            "effective_date": col('s.effective_date'),
            "end_date": lit(None).cast(TimestampType())
        }
    ).\
    execute()

# COMMAND ----------

df.write.mode('append').saveAsTable('nyc_taxi.02_silver.taxi_zone_lookup')