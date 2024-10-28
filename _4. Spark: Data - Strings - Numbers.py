# Databricks notebook source
# MAGIC %md
# MAGIC # Import libraries

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC # Read Data

# COMMAND ----------

df_orders = spark.read.table("hive_metastore.sales.orders")


# COMMAND ----------


