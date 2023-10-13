# Databricks notebook source
# MAGIC %md
# MAGIC # Batch - Kafka to Bronze

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
# MAGIC Ingestion is made by using ingest method of the ingestion_helper module. Note that availableNow is enable because these are batch tables.

# COMMAND ----------

catalog = "training"
schema = "pos_bronze"
trigger = {"availableNow": True, "processingTime": None}

topic = "prod.items"
table = "items"
ingest(spark, topic, catalog, schema, table, trigger)

topic = "prod.stores"
table = "stores"
ingest(spark, topic, catalog, schema, table, trigger)

topic = "prod.inventory.types"
table = "inventory_types"
ingest(spark, topic, catalog, schema, table, trigger)

topic = "prod.inventory.snapshots"
table = "inventory_snapshots"
ingest(spark, topic, catalog, schema, table, trigger)
