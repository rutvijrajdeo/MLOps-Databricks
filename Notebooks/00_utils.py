# Databricks notebook source
# MAGIC %md
# MAGIC ### Import Libraries

# COMMAND ----------

import numpy as np
import pandas as pd
pd.set_option('display.max_rows', 1200)
pd.set_option('display.max_columns', 100)
import statsmodels
from statsmodels.tsa.holtwinters import ExponentialSmoothing

# COMMAND ----------

import matplotlib.pyplot as plt
%matplotlib inline
import seaborn as sns
from cycler import cycler

# COMMAND ----------

import os
import subprocess
from functools import reduce
import time
from time import perf_counter
from datetime import datetime
import datetime
from datetime import date, timedelta
import warnings
warnings.filterwarnings('ignore')
import re
from itertools import chain

# COMMAND ----------


import pyspark
from pyspark.sql import functions as F
import pyspark.sql.types as T
from pyspark.sql import DataFrame
from pyspark.mllib.stat import Statistics
import pyspark.pandas as ps
from pyspark.sql import Window
import pyspark.sql.functions as func
from pyspark.sql.functions import (to_timestamp, date_format, to_date, last_day, 
                                  unix_timestamp, datediff, when)
from pyspark.sql.functions import col,isnan, when, count, countDistinct
from pyspark.sql.functions import expr, sum as spark_sum
from pyspark.ml.stat import Correlation
from pyspark.ml.linalg import DenseMatrix, Vectors
from pyspark.ml.feature import VectorAssembler
from pyspark.sql.functions import *
from pyspark.sql.types import (StructType, StructField, StringType, ByteType, IntegerType, 
                               FloatType, DateType, LongType, DecimalType, DoubleType, NumericType)
from pyspark.sql.types import DoubleType,StringType
from pyspark.sql.functions import dayofweek, month, quarter, year, dayofmonth
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number
from pyspark.ml.pipeline import Transformer
from pyspark import SparkConf
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import IntegerType, FloatType


# COMMAND ----------

conf = SparkConf()
# conf.set("spark.driver.memory", "140g") 
# conf.set("spark.executor.memory", "140g")
# conf.set("spark.executor.cores", "20")
spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")
conf.set("spark.sql.execution.arrow.enabled", "true")
conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
spark.conf.set("fs.azure.account.auth.type.mlopsstorage1705.dfs.core.windows.net", "SAS")
spark.conf.set("fs.azure.sas.token.provider.type.mlopsstorage1705.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set("fs.azure.sas.fixed.token.mlopsstorage1705.dfs.core.windows.net", "sv=2022-11-02&ss=bfqt&srt=sco&sp=rwdlacupyx&se=2024-05-31T14:54:15Z&st=2024-04-30T06:54:15Z&spr=https&sig=906SjfPG6tFLNLsgXx4W2XAccS%2BdKxBOD8W7xpfPqzE%3D")
pd.set_option('display.float_format', '{:.2f}'.format)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### User Define Functions

# COMMAND ----------

def read_parquet(direct_path, reqd_cols, frmt='parquet', recur_file_look='true'):
    

    return spark.read.format(frmt)\
                     .option("recursiveFileLookup", recur_file_look)\
                     .load(direct_path)\
                     .select(reqd_cols)

# COMMAND ----------

def write_parquet(folder_path, file,file_name , adls_storage_account="devadlednaeus2", container="landing"):

    return file.write.mode('overwrite')\
                     .parquet(f'abfss://{container}@{adls_storage_account}.dfs.core.windows.net/Localization/{folder_path}/{file_name}') 

# COMMAND ----------

def read_csv(file_path):

    return spark.read.option('header', True).csv(file_path)

# COMMAND ----------

def read_deltaTable(delta_filepath):    

    return spark.read.format("delta").load(delta_filepath)

# COMMAND ----------

def write_deltaTable(py_df, zorder_col, delta_filepath, over_write_schema ="false", data_change ="true", mode="overwrite"):
    
    return py_df.write.format("delta").mode(mode)\
                                      .option("ZOrderBy", zorder_col)\
                                      .option("overWriteSchema", over_write_schema)\
                                      .option("dataChange", data_change)\
                                      .save(delta_filepath)
