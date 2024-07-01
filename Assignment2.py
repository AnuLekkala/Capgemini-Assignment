# Databricks notebook source
# MAGIC %md 
# MAGIC ###Assignment 2 
# MAGIC ####Calculate CurrentBalance

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, when, trim
from pyspark.sql.window import Window

def CalBalance(path):
  # Read i/p
  df = spark.read.format("csv").option("header","true").option("sep",";").load(path)
  
  #trim whitespaces from TransactionType
  df = df.withColumn("TransactionType", trim(df.TransactionType))

  # Calculate the Current Balance
  df = df.withColumn("CurrentBalance",sum(when(df.TransactionType == "Credit", df.Amount).otherwise(-df.Amount)).over(Window.partitionBy("AccountNumber").orderBy("TransactionDate")))
  return df

path = "/FileStore/tables/Dataset.csv"
result = CalBalance(path)
result.display()

# COMMAND ----------


