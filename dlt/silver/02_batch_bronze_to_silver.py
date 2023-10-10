# Databricks notebook source
# MAGIC %md
# MAGIC # Batch - Bronze to Silver

# COMMAND ----------

# MAGIC %md
# MAGIC In this step, the table is deduplicated and saved in the silver layer.

# COMMAND ----------

import pyspark
from pyspark.sql.functions import to_timestamp

# COMMAND ----------

# MAGIC %md
# MAGIC Catalog and schema names are set for next steps.

# COMMAND ----------

catalog = "training"
schema = "test_pos_silver"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Stores

# COMMAND ----------

table = "stores"
(
    spark.read.table(f"training.test_pos.{table}")
    .select("avro_value.*")
    .write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(f"{catalog}.{schema}.{table}")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Items

# COMMAND ----------

table = "items"
(
    spark.read.table(f"training.test_pos.{table}")
    .select("avro_value.*")
    .write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(f"{catalog}.{schema}.{table}")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Inventory Types

# COMMAND ----------

table = "inventory_types"
(
    spark.read.table(f"training.test_pos.{table}")
    .select("avro_value.*")
    .write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(f"{catalog}.{schema}.{table}")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Inventory Snapshots

# COMMAND ----------

table = "inventory_snapshots"
(
    spark.read.table(f"training.test_pos.{table}")
    .select("avro_value.*")
    .withColumn("date_time", to_timestamp("date_time"))
    .write
    .format("delta")
    .mode("append")
    .saveAsTable(f"{catalog}.{schema}.{table}")
)
