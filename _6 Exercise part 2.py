# Databricks notebook source
# MAGIC %md
# MAGIC # Read Silver

# COMMAND ----------

# Load cleansed data from Silver layer
silver_df = spark.read.table("silver_table")

# COMMAND ----------

# MAGIC %md
# MAGIC # Transform/Aggregate...

# COMMAND ----------



# Example: Aggregate data for reporting
gold_df = (
    silver_df.groupBy("category")
    .agg({"value_column": "sum"})
    .withColumnRenamed("sum(value_column)", "total_value")
)



# COMMAND ----------

# MAGIC %md
# MAGIC # Write to Gold layer

# COMMAND ----------

# Write to Gold layer
gold_df.write.format("delta").mode("overwrite").saveAsTable("gold_table")

# COMMAND ----------


