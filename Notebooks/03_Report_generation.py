# Databricks notebook source
# MAGIC %run ./00_utils

# COMMAND ----------

sales_data = read_deltaTable(
    "abfss://mlops-dataset@mlopsstorage1705.dfs.core.windows.net/Final_dataset"
).drop("Sales_Units")

forecast_data = read_deltaTable(
    "abfss://mlops-dataset@mlopsstorage1705.dfs.core.windows.net/Forecast_Sales_Output"
)
clnd = read_csv(
    "abfss://mlops-dataset@mlopsstorage1705.dfs.core.windows.net/Calendar/Calendar.csv"
).withColumn("week_start", F.col("week_start").cast("date"))

# COMMAND ----------

fcst_data = (
    forecast_data.join(clnd, on="week_start", how="left")
    .withColumn("LY", F.col("year") - 1)
    .withColumn("LLY", F.col("year") - 2)
)
sls_data = sales_data.join(clnd, on="week_start", how="left")

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

compare = (
    final_data.groupBy("week_start")
    .agg(
        F.sum("Forecast_Sales").alias("Forecast_Sales"),
        F.sum("LY_Sales_Units_updated").alias("LY_Sales_Units_updated"),
        F.sum("LLY_Sales_Units_updated").alias("LLY_Sales_Units_updated"),
    )
    .orderBy("week_start")
)

df = compare.toPandas()

# Plotting
plt.figure(figsize=(18, 12))

plt.plot(df["week_start"], df["Forecast_Sales"], label="Forecast Sales")
plt.plot(df["week_start"], df["LY_Sales_Units_updated"], label="LY_Sales_Units")
plt.plot(df["week_start"], df["LLY_Sales_Units_updated"], label="LLY_Sales_Units")
plt.title("Forecast Sales vs. LY_Sales_Units  vs.LLY_Sales_Units")
plt.xlabel("Week Start")
plt.ylabel("Sales and Forecast_Sales")
plt.legend()

plt.show()

# COMMAND ----------


