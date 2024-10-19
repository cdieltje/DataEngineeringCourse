# Databricks notebook source
# Restart Kernel
# dbutils.library.restartPython() # run this code in the notebook without restarting the cluster or using pip install again.

# COMMAND ----------

# MAGIC %md
# MAGIC # Import libraries

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC # Read data from a table

# COMMAND ----------

# Load the table from the hive_metastore database
df_orders = spark.read.table("hive_metastore.sales.orders")

# Show the DataFrame content
df_orders.display() # Without '.display()' the task is done, but nothing will be displayed

# COMMAND ----------

# choose only a couple of columns to put in the spark df:
df_limited = spark.read.table("hive_metastore.sales.orders").select('OrderID', 'CustomerID')
df_limited.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Create a Spark dataframe from scratch

# COMMAND ----------

data = [("James", 34), ("Anna", 20), ("Julia", 55)]
columns = ["Name", "Age"]

df_name_age = spark.createDataFrame(data, columns)
df_name_age.display()

# COMMAND ----------

# pip install pyspark
# from.pyspark.sql.function import *

# COMMAND ----------

# MAGIC %md
# MAGIC # Filtering

# COMMAND ----------

# Filtering rows Pandas
# df_name_age[df_name_age['Age'] > 30].display()

df_orders_comment = df_orders.select('OrderID', 'CustomerID', 'Comments', 'CustomerPurchaseOrderNumber')

# Examples with different filter options
print('CustomPurchaseOrderNumber > 19999')
df_orders_comment.filter(F.col('CustomerPurchaseOrderNumber') > 19999).display() 

print('CustomPurchaseOrderNumber = 19999')
df_orders_comment.filter(F.col('CustomerPurchaseOrderNumber')== 19999).display()

print('Comments contain letter \'n\'')
df_orders_comment.filter(F.col('Comments').contains('n')).display() # comments contains 'e'

print('Comments contain letter \'n\', only show 3 rows')
df_orders_comment.filter(F.col('Comments').contains('n')).limit(3).display() # comments contains 'e', and only display 3 first rows

print('Comments contains letter \'n\' wheter it\'s a capital or not using regex (https://sparkbyexamples.com/spark/spark-rlike-regex-matching-examples)')
df_orders_comment.filter(F.col('Comments').rlike('(?i)n')).display() # 'rlike': apply regex filters. '?i)': don't take capitals/ into account.

print('OrderID is one of the listed values + only show columns \'OrderID\' and \'CustomerID\' ')
(df_orders_comment.filter(F.col('OrderID') # also example of 'chaining'
                  .isin([21160, 24036]))
                  .select('OrderID','CustomerID')
                  .display()
                  )

# COMMAND ----------

# MAGIC %md
# MAGIC #### Method chaining
# MAGIC Show OrderID, CustomerID and Comment of the orders where the Comment starts with 'Ik'
# MAGIC

# COMMAND ----------

# Same output, but second is easier to read
df_orders.filter(F.col('Comments').startswith('Ik')).select('OrderID','CustomerID', 'Comments').display()

(df_orders.filter(F.col('Comments')
                 .startswith('Ik'))
                 .select('OrderID','CustomerID', 'Comments')
                 .display()
)

# COMMAND ----------

# WAAR WAS IK IN MAIL => na regex-link
