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
source_schema = "pos_bronze"
target_schema = "pos_silver"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Stores

# COMMAND ----------

source_table = "stores"
target_table = "stores"
(
    spark.read.table(f"{catalog}.{source_schema}.{source_table}")
    .select("avro_value.*")
    .write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(f"{catalog}.{target_schema}.{target_table}")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Items

# COMMAND ----------

source_table = "items"
target_table = "items"
(
    spark.read.table(f"{catalog}.{source_schema}.{source_table}")
    .select("avro_value.*")
    .write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(f"{catalog}.{target_schema}.{target_table}")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Inventory Types

# COMMAND ----------

source_table = "inventory_types"
target_table = "inventory_types"
(
    spark.read.table(f"{catalog}.{source_schema}.{source_table}")
    .select("avro_value.*")
    .write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(f"{catalog}.{target_schema}.{target_table}")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Inventory Snapshots

# COMMAND ----------

source_table = "inventory_snapshots"
target_table = "inventory_snapshots"
(
    spark.read.table(f"{catalog}.{source_schema}.{source_table}")
    .select("avro_value.*")
    .withColumn("date_time", to_timestamp("date_time"))
    .write
    .format("delta")
    .mode("append")
    .saveAsTable(f"{catalog}.{target_schema}.{target_table}")
)
