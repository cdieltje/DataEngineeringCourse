# Databricks notebook source
# MAGIC %md
# MAGIC # Read bronze layer

# COMMAND ----------

bronze_df = spark.read.table("bronze_table")

# COMMAND ----------

# MAGIC %md
# MAGIC # Transform

# COMMAND ----------

# Apply transformations (e.g., deduplication, filtering null values)
silver_df = (
    bronze_df.dropDuplicates(["unique_key"])
    .filter("required_column IS NOT NULL")
)

# COMMAND ----------

# MAGIC %md
# MAGIC # Write to silver layer

# COMMAND ----------

# Write to Silver layer
silver_df.write.format("delta").mode("overwrite").saveAsTable("silver_table")
