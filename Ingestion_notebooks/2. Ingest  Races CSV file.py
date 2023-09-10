# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingesting races.csv file 

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

dbutils.fs.ls("/mnt/formula1projectpg/raw")

# COMMAND ----------

races_df= spark.read.option("header",True).option("InferSchema", True).csv(f"{raw_folder_path}/{v_file_date}/races.csv")

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

# MAGIC %md
# MAGIC #### Renaming the columns

# COMMAND ----------

from pyspark.sql.functions import lit

# COMMAND ----------

races_df_renamed = races_df.withColumnRenamed( "raceId" ,"race_id") \
.withColumnRenamed("circuitId" ,"circuit_id")\
.withColumn("data_source" ,lit(v_data_source))\
.withColumn("file_date", lit(v_file_date))


# COMMAND ----------

# MAGIC %md
# MAGIC ####Add Combine date and time in the dataframe

# COMMAND ----------

from pyspark.sql.functions import to_timestamp,current_timestamp, concat, col, lit

# COMMAND ----------

races_df_concat = races_df_renamed.withColumn("race_timestamp" ,to_timestamp(concat(col("date"),lit(' '),col("time")),'yyyy-MM-dd HH:mm:ss'))

# COMMAND ----------

races_df_concat = add_ingestion_date(races_df_concat)

# COMMAND ----------

# MAGIC %md
# MAGIC ####Get required columns 

# COMMAND ----------

races_selected_df = races_df_concat.select(col("race_id"),col("year"),col("round"),col("circuit_id"),col("name"),col("ingestion_date"),col("data_source"),col("race_timestamp"))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Write to Delta

# COMMAND ----------

races_selected_df.write.mode("overwrite").format("delta").partitionBy("year").saveAsTable("f1_processed.races")

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_processed.races;

# COMMAND ----------

