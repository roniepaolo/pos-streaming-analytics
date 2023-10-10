# Databricks notebook source
# MAGIC %md
# MAGIC # Streaming - Kafka to Bronze

# COMMAND ----------

# MAGIC %md
# MAGIC Confluent Kafka module is necessary for schema fetching and is installed in the beginning of the notebook. This module is global in DLT pipeline.

# COMMAND ----------

# MAGIC %pip install confluent-kafka

# COMMAND ----------

import sys
import os
sys.path.append(os.path.abspath("../utils"))

from ingestion_helper import ingest

# COMMAND ----------

# MAGIC %md
# MAGIC Ingestion is made by using ingest method of the ingestion_helper module. Note that availableNow is disable because this is a streaming table.

# COMMAND ----------

catalog = "training"
schema = "test_pos"
trigger = {"availableNow": None, "processingTime": "0 seconds"}

topic = "prod.inventory.transactions"
table = "inventory_transactions"
ingest(spark, topic, catalog, schema, table, trigger)

# COMMAND ----------


