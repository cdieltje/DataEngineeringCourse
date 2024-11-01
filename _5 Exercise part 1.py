# Databricks notebook source
# MAGIC %md
# MAGIC # Import PySpark function

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC # Read bronze layer

# COMMAND ----------

df_customers = spark.read.table("hive_metastore.sales.customers")
df_invoices = spark.read.table("hive_metastore.sales.invoices")

# COMMAND ----------

# MAGIC %md
# MAGIC # Inspect datasets
# MAGIC e.g. by by getting summary or displaying 5 randomly choosen rows

# COMMAND ----------

# print('Customers dataset inspection:')
# df_customers.summary().display()
# df_customers.orderBy(F.rand()).limit(5).display()

# print('Invoices dataset inspection:')
# df_invoices.summary().display()
# df_invoices.orderBy(F.rand()).limit(5).display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Transform

# COMMAND ----------

# Join two df's, keep only customers with an Invoice since May '16
df_customers_invoices = ((df_customers.select('CustomerID', 'CustomerName'))
                         .join(df_invoices.select('CustomerID', 'InvoiceDate', 'InvoiceID'), on='CustomerID', how='left')
                         .filter(F.col('InvoiceDate') > '2016-04-30')
                         .orderBy('CustomerID')
                         )
df_customers_invoices.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Write to Silver layer as a Delta table

# COMMAND ----------

df_customers_invoices.write.format("delta").mode("overwrite").saveAsTable("silver_customer_invoice")

# COMMAND ----------



# COMMAND ----------

# Select only invoices since 2014
# df_invoices1 = df_invoices.filter(F.col('InvoiceDate') > '2016-04-30')
# df_invoices1.select('CustomerID').distinct().count()


df_customers_invoices.select('CustomerID').distinct().count()
# df_invoices.filter(F.col('InvoiceDate') <= 100 )

# df_customers_invoices.select('InvoiceID').distinct().count()
# df_customers_invoices.filter(F.col("InvoiceID").isNull()).distinct().count()
# df_invoices.filter(F.col('CustomerID').isNotNull()).distinct().count()
# df_invoices.select('CustomerID').distinct().count()
