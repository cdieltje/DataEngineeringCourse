# Databricks notebook source
# MAGIC %md
# MAGIC # Import PySpark function

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC # Read Silver

# COMMAND ----------

# Load cleansed data from Silver layer
df_silver = spark.read.table("silver_customer_invoice") # hive_metastore/default/silver_customer_invoice

# COMMAND ----------

# MAGIC %md
# MAGIC # Transform

# COMMAND ----------

# Add new field InvoiceWeek
df_customer_invoiceweek = silver_df.withColumn('InvoiceWeek', F.weekofyear(F.col('InvoiceDate')))

# Df with weekly invoice count per customer and customer category
df_customer_invoiceweek = (
    df_customer_invoiceweek.groupBy('CustomerID', 'InvoiceWeek')
    .agg({'InvoiceID': 'count'})
    .withColumnRenamed('count(InvoiceID)', 'InvoiceCount')
    # .orderBy('CustomerID', 'InvoiceWeek')
)

# df_customer_invoiceweek.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Write to Gold layer

# COMMAND ----------

df_customer_invoiceweek.write.format("delta").mode("overwrite").saveAsTable("gold_customer_invoiceweek")
