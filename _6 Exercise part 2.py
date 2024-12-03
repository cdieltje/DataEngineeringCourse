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
df_customer_invoiceweek = df_silver.withColumn('InvoiceWeek', F.weekofyear(F.col('InvoiceDate')))

# Df with weekly invoice count per customer and customer category
df_customer_invoiceweek = (
    df_customer_invoiceweek.groupBy('CustomerID', 'InvoiceWeek')
    .agg({'InvoiceID': 'count'})
    .withColumnRenamed('count(InvoiceID)', 'InvoiceCount')
    .withColumn('LastUpdateTime', F.current_timestamp())
    .orderBy('CustomerID', 'InvoiceWeek')
)

df_customer_invoiceweek.display()

# COMMAND ----------

# Transform LastUpdateTime to a string
df_customer_invoiceweek = df_customer_invoiceweek.withColumn('LastUpdateTime', F.col('LastUpdateTime').cast('string'))
df_customer_invoiceweek.display()

# COMMAND ----------

# Transform LastUpdateTime back to a date with specified format
df_customer_invoiceweek = df_customer_invoiceweek.withColumn('LastUpdateTime', F.date_format(F.to_timestamp('LastUpdateTime', 'yyyy-MM-dd HH:mm:ss.SSS'), 'yyyy/MM/dd'))

df_customer_invoiceweek.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Write to Gold layer

# COMMAND ----------

# Save as delta table
(df_customer_invoiceweek.write.format("csv")
 .mode("overwrite")
 .option("mergeSchema", "true") # prevent errors in case of schema changes 
 .saveAsTable("gold_customer_invoiceweek")
)

# # Create view on top of delta table
# spark.sql("""
# CREATE MATERIALIZED VIEW customer_invoiceweek_mv
# AS
# SELECT *
# FROM gold_customer_invoiceweek
# """)

# # View:
# spark.sql("""
# CREATE OR REPLACE VIEW gold_customer_invoiceweek AS
# SELECT *
# FROM gold_customer_invoiceweek
# """)

# COMMAND ----------

# df_customer_invoiceweek.createOrReplaceTempView("df_customer_invoiceweek")
df_customer_invoiceweek.createOrReplaceTempView("df_customer_invoiceweek")

# Share as a view
spark.sql("""
CREATE OR REPLACE TEMP VIEW customer_invoiceweek_view AS -- temp view as we have a tempview created above
SELECT *
FROM df_customer_invoiceweek
""")

# COMMAND ----------


