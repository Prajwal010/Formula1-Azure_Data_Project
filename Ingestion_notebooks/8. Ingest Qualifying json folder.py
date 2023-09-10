# Databricks notebook source
# MAGIC %md
# MAGIC #### Read multiple json from qualifying folder

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

qualifying_schema = StructType(fields=[StructField("qualifyId", IntegerType(), False),
                                      StructField("raceId", IntegerType(), True),
                                      StructField("driverId", IntegerType(), True),
                                      StructField("constructorId", IntegerType(), True),
                                      StructField("number", IntegerType(), True),
                                      StructField("position", IntegerType(), True),
                                      StructField("q1", StringType(), True),
                                      StructField("q2", StringType(), True),
                                      StructField("q3", StringType(), True),
                                     ])


# COMMAND ----------

qualifying_df = spark.read.schema(qualifying_schema).option('multiline', True).json(f"{raw_folder_path}/{v_file_date}/qualifying")

# COMMAND ----------

# MAGIC %md
# MAGIC ####Rename column and add timestamp column

# COMMAND ----------

from pyspark.sql.functions import current_timestamp , col,lit

# COMMAND ----------

qualifying_with_ingestion_date_df = add_ingestion_date(qualifying_df)

# COMMAND ----------

qualifying_final_df = qualifying_with_ingestion_date_df.withColumnRenamed('qualifyId','qualify_id')\
                                   .withColumnRenamed('raceId','race_id')\
                                   .withColumnRenamed('driverId','driver_id')\
                                   .withColumnRenamed('constructorId','constructor_id')\
                                   .withColumn("ingestion_date", current_timestamp()) \
                                   .withColumn("data_source" ,lit(v_data_source))\
                                   .withColumn("file_date" ,lit(v_file_date))                               

# COMMAND ----------

# MAGIC %md
# MAGIC #### Write to Parquet

# COMMAND ----------

merge_condition = "tgt.qualify_id = src.qualify_id AND tgt.race_id = src.race_id"
merge_delta_data(qualifying_final_df, 'f1_processed', 'qualifying', processed_folder_path, merge_condition, 'race_id')

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------

