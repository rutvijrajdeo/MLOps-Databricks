# Databricks notebook source
# MAGIC %run ./00_utils

# COMMAND ----------

# MAGIC %md
# MAGIC ### Read Historical Data

# COMMAND ----------

hist_data = read_csv(
    "abfss://mlops-dataset@mlopsstorage1705.dfs.core.windows.net/Histiorical_dataset"
)
hist_data.display()

# COMMAND ----------

hist_data = read_csv(
    "abfss://mlops-dataset@mlopsstorage1705.dfs.core.windows.net/Histiorical_dataset"
)
hist_data = (
    hist_data.withColumn("week_start", F.col("week_start").cast("date"))
    .withColumn("Sales_Units", F.col("Sales_Units").cast("integer"))
    .withColumn("Sales_Units_rand", F.col("Sales_Units_rand").cast("integer"))
    .withColumnRenamed("Sales_Units_rand", "Sales_Units_updated")
)

hist_data.groupBy("Class").agg(F.min("week_start"), F.max("week_start")).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Read Incremental Data

# COMMAND ----------

inc_data = read_csv(
    "abfss://mlops-dataset@mlopsstorage1705.dfs.core.windows.net/Incremental_dataset"
)
inc_data = (
    inc_data.withColumn("week_start", F.col("week_start").cast("date"))
    .withColumn("Sales_Units", F.col("Sales_Units").cast("integer"))
    .withColumn("Sales_Units_rand", F.col("Sales_Units_rand").cast("integer"))
    .withColumnRenamed("Sales_Units_rand", "Sales_Units_updated")
)

inc_data.groupBy("Class").agg(F.min("week_start"), F.max("week_start")).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Consolidate HIstorical and Incremental Data

# COMMAND ----------

final_data = hist_data.unionByName(inc_data)

# COMMAND ----------

final_data.display()

# COMMAND ----------

final_data.count(), final_data.dropDuplicates().count()

# COMMAND ----------

final_data.groupBy("Class").agg(F.min("week_start"), F.max("week_start")).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Write Final data in delta file

# COMMAND ----------

write_deltaTable(
    py_df=final_data,
    zorder_col=["Class", "Store", "week_start"],
    delta_filepath="abfss://mlops-dataset@mlopsstorage1705.dfs.core.windows.net/Final_dataset",
)
