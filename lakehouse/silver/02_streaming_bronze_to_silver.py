# Databricks notebook source
# MAGIC %md
# MAGIC # Streaming - Bronze to Silver

# COMMAND ----------

# MAGIC %md
# MAGIC In this step, the table is deduplicated and saved in the silver layer.

# COMMAND ----------

import pyspark
from pyspark.sql.functions import regexp_extract, to_timestamp, countDistinct

# COMMAND ----------

# MAGIC %md
# MAGIC Catalog, schema and table names are set for next steps.

# COMMAND ----------

catalog = "training"
schema = "test_pos_silver"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Inventory Transactions

# COMMAND ----------

# MAGIC %md
# MAGIC Structured Stream is processed and saved in the silver layer.

# COMMAND ----------

source_table = "inventory_transactions2"
target_table = "inventory_transactions4"
(
    spark.readStream.table(f"training.test_pos.{source_table}")
    .select("avro_value.*")
    .withColumns(
        {
            "trans_id": regexp_extract("trans_id", "\{(.*)\}", 1),
            "date_time": to_timestamp("date_time"),
        }
    )
    .withWatermark("date_time", "1 hour")
    .dropDuplicates(["trans_id", "item_id"])
    .writeStream.format("delta")
    .outputMode("append")
    .option(
        "checkpointLocation",
        f"/user/hive/warehouse/{schema}/{target_table}/_checkpoints",
    )
    .queryName(f"{catalog}_{schema}_{target_table}")
    .toTable(f"{catalog}.{schema}.{target_table}")
)
