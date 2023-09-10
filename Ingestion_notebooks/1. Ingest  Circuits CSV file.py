# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingesting circuits.csv file 

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

circuits_df= spark.read.option("header",True).option("InferSchema", True).csv(f"{raw_folder_path}/{v_file_date}/circuits.csv")

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

circuits_df = circuits_df.select(col("circuitId"),col("circuitRef"),col("name"),col("location"),col("lat"),col("lng"),col("alt"))
display(circuits_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Renaming the columns

# COMMAND ----------

from pyspark.sql.functions import lit

# COMMAND ----------

circuits_df_renamed = circuits_df.withColumnRenamed( "circuitId" ,"circuit_id") \
.withColumnRenamed("circuitRef" ,"circuit_ref")\
.withColumnRenamed("lat" ,"latitude")\
.withColumnRenamed("lng" ,"longitude")\
.withColumnRenamed("alt" ,"altitude")\
.withColumn("data_source" ,lit(v_data_source))\
.withColumn("file_date", lit(v_file_date))


# COMMAND ----------

# MAGIC %md
# MAGIC ####Add ingestion date to the dataframe using workflow function

# COMMAND ----------

circuits_final_df = add_ingestion_date(circuits_df_renamed)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Write to Parquet

# COMMAND ----------

circuits_final_df.write.mode("overwrite").format("delta").saveAsTable("f1_processed.circuits")

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/formula1projectpg/processed/circuits

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_processed.circuits

# COMMAND ----------

