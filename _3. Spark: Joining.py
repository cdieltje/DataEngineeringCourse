# Databricks notebook source
# MAGIC %md
# MAGIC # Import libraries

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC # Read Data
# MAGIC We do it on a small selection. There's a reason.

# COMMAND ----------

# Read selection of two datasets (Get orders and OrderLines for OrderID <= 100)
df_orders = (spark.read.table("hive_metastore.sales.orders")
             .filter(F.col('OrderID') <= 100 )
             .select('OrderID', 'CustomerID')
             )
df_orderlines = (spark.read.table("hive_metastore.sales.orderlines")
                 .filter(F.col('OrderID') <= 100 )
                 .select('OrderLineID', 'OrderID', 'Description')
)

print('Display 3 rows of df_orders:')
df_orders.limit(3).display()

# COMMAND ----------

df_orders_orderlines = df_orders.join(df_orderlines, on='OrderID', how='left')
print('Display 3 rows of df_orders after join:')
df_orders_orderlines.limit(3).display()

# All usuals joins are supported, eg via how='inner', how='outer'

# COMMAND ----------

# MAGIC %md
# MAGIC # Cost
# MAGIC Joins = costly, especially when working with distrubion.
# MAGIC
# MAGIC The data for each joining key may not be located on the same node and requires costly shuffling/data transfers.
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Broadcasting
# MAGIC (https://sparkbyexamples.com/pyspark/pyspark-broadcast-join-with-example/)
# MAGIC - Variables are cached in serialized form and can be reused across multiple operations on the same RDD (Resilient Distributed Dataset)
# MAGIC - By broadcasting the variables, Spark ensures that each node in the cluster has a copy of the data locally, reduces costly data transfers.
# MAGIC - Data for each join key is available on every node
# MAGIC - Useful if larger df is joined with smaller df, less useful for joining two larger df's (which would not fit in memory of each node)
# MAGIC - PySpark SQL function

# COMMAND ----------

print('No broadcast example (to compare runtinme):')
df_orders_orderlines = df_orders.join(df_orderlines, "OrderID", "left")

# More verbose:
# df_orders_orderlines = (df_orders.join(df_orderlines,(df_orders['OrderID'] == df_orderlines['OrderID']), how = 'left')
#                                        .drop(df_orderlines['OrderID'])
#                                        )

df_orders_orderlines.limit(3).display()

# COMMAND ----------

print('Broadcast example:')
df_orders_orderlines = (df_orders.join(F.broadcast(df_orderlines),(df_orders['OrderID'] == df_orderlines['OrderID']))
                                       .drop(df_orderlines['OrderID'])
                                       )
df_orders_orderlines.limit(3).display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Filtering with conditions

# COMMAND ----------

# Create df with join and filter the df in one go. Filtering possible with both of original df's
df_orders_some_orderlines = (df_orders.join(df_orderlines,
                                           (df_orders['OrderID'] == df_orderlines['OrderID']) # other syntax in place of 'on=...'
                                           & (df_orders['OrderID'] < 10)
                                           )
                                           .drop(df_orderlines['OrderID']) # other 'on' syntax above leads to two columns with same name OrderID, which could lead to errors, eg. if filtered on OrderID as below. therefore: drop or rename on beforehand
                            )

# print('Verification 1: Display 3 rows of filtered df_orders: OrderID\'s below 10:)')
# (df_orders_some_orderlines.limit(100)
#  .filter(F.col('OrderID') <= 10)
#  .limit(10)
#  .display()
# )

# print('Verification 2: Display 3 rows of filtered df_orders: No OrderID\'s above 10:')
# (df_orders_some_orderlines.limit(100)
#  .filter(F.col('OrderID') > 10)
#  .limit(10)
#  .display()
# )

print('Multiple combinations of joining + filtering are possible: Filter on value in first df:')

(df_orders.filter(F.col('CustomerID').startswith('8')) 
 .join(df_orderlines, how = "left")
 .drop(df_orderlines['OrderID'])
 .display()
)

print('Filter on value in second df: display rows where \'Description\' starts with \'USB\':')
print('Lazy evaluation when filter on second df is applied inside join operation. PySpark recognizes this and chooses the most efficent path = filter BEFORE join:')
(df_orders
 .join(df_orderlines.filter(F.col('Description').startswith('USB')), how = "left")
 .drop(df_orderlines['OrderID'])
 .display()
)

# COMMAND ----------

df_orderlines1 = df_orderlines.filter(F.col('Description').startswith('USB'))

print('Filter on value in second df: display rows where \'Description\' starts with \'USB\':')
(df_orders
 .join(df_orderlines, how = "left")
 .drop(df_orderlines['OrderID'])
 .display()
)

# COMMAND ----------


