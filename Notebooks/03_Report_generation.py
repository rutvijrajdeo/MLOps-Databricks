# Databricks notebook source
# MAGIC %run ./00_utils

# COMMAND ----------

sales_data = read_deltaTable("abfss://mlops-dataset@mlopsstorage1705.dfs.core.windows.net/Final_dataset").drop('Sales_Units')
forecast_data = read_deltaTable("abfss://mlops-dataset@mlopsstorage1705.dfs.core.windows.net/Forecast_Sales_Output")
clnd = read_csv("abfss://mlops-dataset@mlopsstorage1705.dfs.core.windows.net/Calendar/Calendar.csv").withColumn("week_start", F.col("week_start").cast('date'))

# COMMAND ----------

fcst_data = forecast_data.join(clnd, on='week_start', how='left').withColumn('LY',F.col('year')-1).withColumn('LLY',F.col('year')-2)
sls_data = sales_data.join(clnd, on='week_start', how='left')


# COMMAND ----------

final_data = fcst_data.join(
    sls_data.select("Class", "Store", "Year", "Week", "Sales_Units_updated")
    .withColumnRenamed("Year", "LY")
    .withColumnRenamed("Sales_Units_updated", "LY_Sales_Units_updated")
    .distinct(),
    on=["Class", "Store", "LY", "Week"],
    how="left",
).join(
    sls_data.select("Class", "Store", "Year", "Week", "Sales_Units_updated")
    .withColumnRenamed("Year", "LLY")
    .withColumnRenamed("Sales_Units_updated", "LLY_Sales_Units_updated")
    .distinct(),
    on=["Class", "Store", "LLY", "Week"],
    how="left",
)

# COMMAND ----------

final_data.display()

# COMMAND ----------


