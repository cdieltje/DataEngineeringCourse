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
# MAGIC # Column transformations

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

# Rename column
print('Rename column BackorderOrderID:')
df_orders_bostatus =  df_orders_bostatus.withColumnRenamed("BackorderOrderID", "B_O_ID")
df_orders_bostatus.limit(5).display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Aggregating

# COMMAND ----------

df_orders_salesperson = df_orders.select('OrderID', 'CustomerID', 'SalespersonPersonID', 'OrderDate')

# Grouping with one aggregation
print('row count per SalesPerson = count of OrderID\' per SalesPerson:') # every row in df stands for one OrderID
orders_salesperson = df_orders_salesperson.groupBy('SalespersonPersonID').count() # aggregation like .count needed, otherwise a groupby object is made, which you can't just display
orders_salesperson.display()
print("")

# Grouping with multiple aggregations
print('Overview of SalesPerson, their row count and their first OrderDate:')
orders_salesperson_firstorderdate = (df_orders_salesperson.groupBy('SalespersonPersonID').agg(
    F.count('SalespersonPersonID').alias('OrderID count'),
    F.min('OrderDate').alias('First Orderdate'))
    )
orders_salesperson_firstorderdate.orderBy(orders_salesperson_firstorderdate['OrderID count'].desc()).display()
print("")

# Grouping with multiple fields
print('Goupby SalesPerson and CustomerID:')
orders_salesperson_customer = (df_orders_salesperson.groupBy('SalespersonPersonID', 'CustomerID')
                               .count()
                               .orderBy(F.col('count').desc()) # F.col is a function that converts column name from string type to Column type. It returns a column
                               )

# orders_salesperson_customer.orderBy(orders_salesperson_customer['count'].desc()).display()
orders_salesperson_customer.display()

print("")

# Other combinations...
print('SalesPersons with ID higher than 20 and their row count:')
orders_salesperson = (df_orders_salesperson.groupBy('SalespersonPersonID')
                      .count()
                      .filter(F.col('count') > 7300)
                    )
orders_salesperson.display()


# COMMAND ----------

# Define custom aggregation functions using User Defined Function (udf) and DoubleType functions = absurd example to show udf use
from pyspark.sql.types import DoubleType

def custom_agg_func(values_list):
	return sum(values_list) / len(values_list) # Example: simple average

udf_custom_agg = udf(custom_agg_func, DoubleType()) # DoubleType needed to avoid type matching error

(df_orders_salesperson.groupBy('SalespersonPersonID')
 .agg(udf_custom_agg(F.collect_list('OrderID')).alias("Average OrderID") # .collect_list after groupBy makes list of values for each group
).display())

# COMMAND ----------


