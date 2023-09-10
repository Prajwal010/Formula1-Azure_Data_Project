# Databricks notebook source
v_result = dbutils.notebook.run("1.ingest_circuits_file", 0, {"p_data_source": "Ergast API", "p_file_date": "2021-04-18"})

# COMMAND ----------

v_results

# COMMAND ----------

v_results =dbutils.notebook.run("2. Ingest  Races CSV file", 0, {"p_data_source": "Ergast API", "p_file_date": "2021-04-18"})

# COMMAND ----------

v_results

# COMMAND ----------

v_results =dbutils.notebook.run("3. Ingest Constructor json file", 0, {"p_data_source": "Ergast API", "p_file_date": "2021-04-18"})

# COMMAND ----------

v_results

# COMMAND ----------

v_results =dbutils.notebook.run("4. Ingest Drivers json file", 0, {"p_data_source": "Ergast API", "p_file_date": "2021-04-18"})

# COMMAND ----------

v_results

# COMMAND ----------

v_results =dbutils.notebook.run("5. Ingest Results json file", 0, {"p_data_source": "Ergast API", "p_file_date": "2021-04-18"})

# COMMAND ----------

v_results

# COMMAND ----------

v_results =dbutils.notebook.run("6. Ingest Pit_Stops json file", 0, {"p_data_source": "Ergast API", "p_file_date": "2021-04-18"})

# COMMAND ----------

v_results

# COMMAND ----------

v_results =dbutils.notebook.run("7. Ingest Lap_times csv folder", 0, {"p_data_source": "Ergast API", "p_file_date": "2021-04-18"})

# COMMAND ----------

v_results

# COMMAND ----------

v_results =dbutils.notebook.run("8. Ingest Qualifying json folder", 0, {"p_data_source": "Ergast API", "p_file_date": "2021-04-18"})

# COMMAND ----------

v_results