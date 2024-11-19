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
# MAGIC - e.g. by getting summary or by displaying 5 randomly choosen rows.
# MAGIC - put in comment

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
                        #  .orderBy('CustomerID')
                         )
# df_customers_invoices.display()

# COMMAND ----------

# Get only first 50 rows
df_customers_invoices = df_customers_invoices.limit(50)

# COMMAND ----------

# MAGIC %md
# MAGIC # Write to Silver layer as a Delta table

# COMMAND ----------

df_customers_invoices.write.format("delta").mode("overwrite").saveAsTable("silver_customer_invoice")

# COMMAND ----------

# df_customers_invoices.display()
