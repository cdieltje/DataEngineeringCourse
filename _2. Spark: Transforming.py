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

# MAGIC %md
# MAGIC # Duplicates management

# COMMAND ----------

df_orders_comments = df_orders.where(F.col("Comments").isNotNull())[['OrderID', 'Comments']] # make df where Comments field is filled in
print('Dataframe with orders that are filled in:')
df_orders_comments.display()

# Drop rows where Comment field is duplicate
df_deduplicated_1 = df_orders_comments.dropDuplicates()
print('Deduplication on all fields together => no deduplication')
df_deduplicated_1.display()


df_deduplicated_2 = df_orders_comments.dropDuplicates(['Comments']) # drop duplicates based on client_number
# clients.dropDuplicates(['client_number']).count # drop duplicates based on client_number, and count
print('Deduplication on one field \'Comments\' => deduplication happened:')
df_deduplicated_2.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Adding / Dropping columns

# COMMAND ----------

# Start with new df with OrderID and BackorderID
df_orders_bostatus = df_orders.select('OrderID', 'BackorderOrderID')
print('start with dataframe with two columns:')
df_orders_bostatus.limit(5).display()

# Add column
print('Add column ProcessStatus with boolean value, based on BackorderOrderID:')
df_orders_bostatus =  df_orders_bostatus.withColumn("ProcessStatus", F.col("BackorderOrderID").isNotNull())
df_orders_bostatus.limit(5).display()

# Drop column
print('Drop column ProcessStatus:')
df_orders_bostatus =  df_orders_bostatus.drop("ProcessStatus")
df_orders_bostatus.limit(5).display()


# COMMAND ----------


