# Databricks notebook source
# MAGIC %md
# MAGIC #### Read multiple CSV from lap_times folder

# COMMAND ----------

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

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType

# COMMAND ----------

lap_times_schema = StructType(fields=[StructField("raceId", IntegerType(), False),
                                      StructField("driverId", IntegerType(), True),
                                      StructField("lap", IntegerType(), True),
                                      StructField("position", IntegerType(), True),
                                      StructField("time", StringType(), True),
                                      StructField("milliseconds", IntegerType(), True)
                                     ])

# COMMAND ----------

lap_times_df = spark.read.schema(lap_times_schema).csv(f"{raw_folder_path}/{v_file_date}/lap_times")


# COMMAND ----------

# MAGIC %md
# MAGIC ####Rename column and add timestamp column

# COMMAND ----------

from pyspark.sql.functions import current_timestamp , col,lit

# COMMAND ----------

lap_times_with_ingestion_date_df = add_ingestion_date(lap_times_df)

# COMMAND ----------

lap_times_final_df = lap_times_with_ingestion_date_df.withColumnRenamed('raceId','race_id')\
                                .withColumnRenamed('driverId','driver_id')\
                                .withColumn("ingestion_date", current_timestamp()) \
                                .withColumn("data_source" ,lit(v_data_source))\
                                .withColumn("file_date" ,lit(v_file_date))

# COMMAND ----------

# MAGIC %md
# MAGIC ####write to delta

# COMMAND ----------

merge_condition = "tgt.race_id = src.race_id AND tgt.driver_id = src.driver_id AND tgt.lap = src.lap AND tgt.race_id = src.race_id"
merge_delta_data(lap_times_final_df, 'f1_processed', 'lap_times', processed_folder_path, merge_condition, 'race_id')

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------

# MAGIC %sql 
# MAGIC select race_id, driver_id, lap, count(*)
# MAGIC  from f1_processed.lap_times
# MAGIC  group by 1,2,3

# COMMAND ----------

