# Databricks notebook source
dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-28")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC #### Read Json

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType

# COMMAND ----------

pit_stops_schema = StructType(fields=[StructField("raceId", IntegerType(), False),
                                      StructField("driverId", IntegerType(), True),
                                      StructField("stop", StringType(), True),
                                      StructField("lap", IntegerType(), True),
                                      StructField("time", StringType(), True),
                                      StructField("duration", StringType(), True),
                                      StructField("milliseconds", IntegerType(), True)
                                     ])



# COMMAND ----------

pit_stops_df = spark.read.schema(pit_stops_schema)\
    .option("multiline", True)\
    .json(f"{raw_folder_path}/{v_file_date}/pit_stops.json")

# COMMAND ----------

# MAGIC %md
# MAGIC ####Rename column and add timestamp column

# COMMAND ----------

from pyspark.sql.functions import current_timestamp , col, lit

# COMMAND ----------

pit_stops_with_ingestion_date_df = add_ingestion_date(pit_stops_df)

# COMMAND ----------

pit_stop_final_df = pit_stops_with_ingestion_date_df.withColumnRenamed('raceId','race_id')\
                                .withColumnRenamed('driverId','driver_id')\
                                .withColumn("ingestion_date", current_timestamp()) \
                                .withColumn("data_source" ,lit(v_data_source))\
                                .withColumn("file_date", lit(v_file_date))                               

# COMMAND ----------

merge_condition = "tgt.race_id = src.race_id AND tgt.driver_id = src.driver_id AND tgt.stop = src.stop AND tgt.race_id = src.race_id"
merge_delta_data(pit_stop_final_df, 'f1_processed', 'pit_stops', processed_folder_path, merge_condition, 'race_id')

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------

# MAGIC %sql 
# MAGIC select race_id, driver_id, stop, count(*)
# MAGIC  from f1_processed.pit_stops
# MAGIC  group by 1,2,3
# MAGIC  order by 1 desc

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM f1_processed.pit_stops
# MAGIC where race_id = 1053

# COMMAND ----------

