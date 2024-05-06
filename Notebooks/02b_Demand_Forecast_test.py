# Databricks notebook source
# MAGIC %run ./00_utils

# COMMAND ----------

final_data = read_deltaTable("abfss://mlops-dataset@mlopsstorage1705.dfs.core.windows.net/Final_dataset")

# COMMAND ----------

def forecast_rolling_13_weeks(group):
    model = ExponentialSmoothing(group['Sales_Units_updated'], seasonal='additive', seasonal_periods=13)
    fitted_model = model.fit()
    forecast = fitted_model.forecast(steps=15)
    # Generate a date range for each forecast step
    # forecast_dates = pd.date_range(start=group['week_start'].max() + pd.Timedelta(days=7), periods=13, freq='W')
    forecast_dates = pd.date_range(start=group['week_start'].max(), periods=13, freq='W')+pd.Timedelta(days=1)
    # Create a dataframe with forecast and corresponding date
    forecast_df = pd.DataFrame({'Class':group['Class'][0],'Store':group['Store'][0],'Forecast_Sales': forecast, 'week_start': forecast_dates})
    return forecast_df

# COMMAND ----------

data = final_data.select('Class','Store','week_start','Sales_Units_updated').orderBy('Class','Store','week_start').filter(F.col('week_start')<'2024-01-16').filter(F.col('Class').isin(['Class1','Class2']))

# COMMAND ----------

schema = StructType([StructField('Class', StringType(), True), StructField('Store', StringType(), True), StructField('week_start', DateType(), True), StructField('Forecast_Sales', IntegerType(), True)])

# COMMAND ----------

output = data.groupby(['Class','Store']).applyInPandas(forecast_rolling_13_weeks, schema=schema)

output.display()

# COMMAND ----------

output.groupBy('Class','Store').agg(F.countDistinct('week_start'), F.min('week_start'),F.max('week_start')).display()

# COMMAND ----------

write_deltaTable(py_df = output,
                 zorder_col = ['Class','Store','week_start'],
                 delta_filepath = 'abfss://mlops-dataset@mlopsstorage1705.dfs.core.windows.net/test_output')

# COMMAND ----------

compare = output.join(
    final_data.select("Class", "Store", "week_start", "Sales_Units_updated"),
    on=["Class", "Store", "week_start"],
    how="left",
).withColumn('wMAPE', abs(F.col('Forecast_Sales')-F.col('Sales_Units_updated'))/F.col('Sales_Units_updated'))

compare.display()

# COMMAND ----------


