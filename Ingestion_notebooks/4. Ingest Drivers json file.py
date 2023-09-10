# Databricks notebook source
# MAGIC %md
# MAGIC #### Read nested Drivers.json

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

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType

# COMMAND ----------

name_schema = StructType( fields=[ StructField("forename", StringType(), True),
                                  StructField("surname", StringType(), True)

])

# COMMAND ----------

display_schema = StructType( fields =[ StructField("driverId", IntegerType(), False),
StructField("driverRef", StringType(), True),
StructField("number", IntegerType(), True),
StructField("code", StringType(), True),
StructField("name", name_schema),
StructField("dob", DateType(), True),
StructField("nationality", StringType(), True),
StructField("url", StringType(), True)
])


# COMMAND ----------

drivers_df = spark.read.schema(display_schema).json(f"{raw_folder_path}/{v_file_date}/drivers.json")

# COMMAND ----------

# MAGIC %md
# MAGIC ####Rename column and add timestamp column & concat name,surname

# COMMAND ----------

from pyspark.sql.functions import current_timestamp , col ,concat, lit

# COMMAND ----------

drivers_renamed_df = drivers_df.withColumnRenamed('driverId','driver_id')\
                                .withColumnRenamed('driverRef','driver_ref')\
                                .withColumn('name',concat(col("name.forename"),lit(" "),col("name.surname")))\
                                .withColumn("data_source" ,lit(v_data_source))\
                                .withColumn("file_date" ,lit(v_file_date))
         

# COMMAND ----------

drivers_renamed_df= add_ingestion_date(drivers_renamed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ####drop unwanted column

# COMMAND ----------

drivers_final_df = drivers_renamed_df.drop("url")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Write to delta

# COMMAND ----------

drivers_final_df.write.mode("overwrite").format("delta").saveAsTable("f1_processed.drivers")

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_processed.drivers