# Databricks notebook source
# MAGIC %md
# MAGIC # Import libraries

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC # Read Data

# COMMAND ----------

df_orders_date = (spark.read.table("hive_metastore.sales.orders")
                  .select('OrderID', 'CustomerID', 'SalespersonPersonID', 'OrderDate', 'LastEditedWhen') # select some date and timestamp columns
                 )

# COMMAND ----------

# MAGIC %md
# MAGIC # Date transformations

# COMMAND ----------

# Display 5 rows
df_orders_date.orderBy(F.rand()).limit(5).display() # show 5 random rows via rand function

# COMMAND ----------

# Extracting date parts via year month, etc function
df_orders_year = df_orders_date.select('OrderID', 'CustomerID', 'SalespersonPersonID', 'OrderDate',
    F.year(F.col('OrderDate')).alias('Year'),
	F.month(F.col('OrderDate')).alias('Month'),
	F.dayofmonth(F.col('OrderDate')).alias('Day')
)

df_orders_year.display()

# Get all orderdates between 2014 and 2015
print('Display only rows of year 2014 and 2015:')
(df_orders_year.filter((F.col('Year').between(2014,2015))) # borders included
 .orderBy('Year')
 .display())

# COMMAND ----------

# Get highest year on top
print('Sort by highest year:')
(df_orders_year.filter((F.col('Year').between(2014,2015))) # borders included
 .orderBy(F.desc('Year'))
 .display())

# COMMAND ----------

print('The same result can be achieved via ascending = False')
(df_orders_year.filter((F.col('Year').between(2014,2015)))
 .orderBy('Year', ascending = False)
 .display())

# COMMAND ----------

# MAGIC %md
# MAGIC ##### On two options above:
# MAGIC - First one 'F.desc' is faster than 'ascending = False'
# MAGIC - F.desc: is a PySpark expression (see 'F.') <> ascending = False: higher-level API abstraction that needs to be converted into 'F.'-function
# MAGIC - If not sure: Test / look up online, or in documentation available in notebook, e.g. via ? of help()

# COMMAND ----------

F.desc?

# COMMAND ----------

ascending=False?

# COMMAND ----------

# Adding and subtracting

# Add 5 days to OrderDate (date_add function)
df_orders_date = df_orders_date.withColumn('Date_plus_5', F.date_add(F.col('OrderDate'),5))

# Subtract 5 days (date_sub function)
df_orders_date = df_orders_date.withColumn('Date_min_5', F.date_sub(F.col('OrderDate'),5))

df_orders_date.display()

# Interval between dates (datediff function)
df_orders_date.withColumn('Days_Between', F.datediff(F.col('Date_plus_5'), F.col('Date_min_5'))).display() # silly example, anyway useful function for leadtimes etc

# COMMAND ----------

# MAGIC %md
# MAGIC ## Format conversions

# COMMAND ----------

# From Date to string

# Convert date column to string in the specified format
from pyspark.sql.functions import date_format
df_orders_date = df_orders_date.select('OrderID', 'CustomerID', 'SalespersonPersonID', 'OrderDate', 'LastEditedWhen', date_format(F.col('LastEditedWhen'),'yyyy/MM/dd hh:mm:ss').alias('OrderDateString')) # check type logo in Table 

df_orders_date.display()

# COMMAND ----------

# From string to date
df_orders_date = df_orders_date.withColumn('OrderDateFormat', F.to_timestamp(F.col('OrderDateString'))) # OrderDateDateFormat becomes duplicate of OrderDate
df_orders_date.display()

# For timestamps => if OrderDateString would be in timestamp format:
# df_orders_date = df_orders_date.withColumn('OrderDateDateFormat', F.to_date(F.col('OrderDateString'),'yyyy/MM/dd HH:mm:ss'))
# df_orders_date.display()

# COMMAND ----------


