from databricks.sdk.runtime import *

import configparser
from pathlib import Path

import pyspark
from confluent_kafka.schema_registry import SchemaRegistryClient
from pyspark.sql.avro.functions import from_avro
from pyspark.sql.functions import col


def init_confs():
    conf_parser = configparser.ConfigParser()
    conf_parser.read(Path("../../ingestion/config/conf.properties"))
    cc_conf = {
        "bootstrap.servers": conf_parser["CONFLUENT"]["bootstrap.servers"],
        "security.protocol": conf_parser["CONFLUENT"]["security.protocol"],
        "sasl.mechanisms": conf_parser["CONFLUENT"]["sasl.mechanisms"],
        "sasl.username": conf_parser["CONFLUENT"]["sasl.username"],
        "sasl.password": conf_parser["CONFLUENT"]["sasl.password"],
    }
    sr_conf = {
        "url": conf_parser["SCHEMA_REGISTRY"]["schema.registry.url"],
        "basic.auth.user.info": conf_parser["SCHEMA_REGISTRY"][
            "basic.auth.user.info"
        ],
    }
    return cc_conf, sr_conf


def get_schemas(sr_client: SchemaRegistryClient, key_subject, value_subject):
    key_schema = sr_client.get_latest_version(key_subject).schema.schema_str
    value_schema = sr_client.get_latest_version(
        value_subject
    ).schema.schema_str
    return key_schema, value_schema


def ingest(spark, topic, catalog, schema, table, trigger):
    login_module_class = (
        "kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule"
    )
    confs = init_confs()
    cc_conf = confs[0]
    sr_conf = confs[1]
    sr_client = SchemaRegistryClient(sr_conf)

    topic_schema = get_schemas(
        sr_client,
        f"{topic}-key",
        f"{topic}-value",
    )

    (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", cc_conf["bootstrap.servers"])
        .option("kafka.security.protocol", cc_conf["security.protocol"])
        .option(
            "kafka.sasl.jaas.config",
            (
                f"{login_module_class} "
                "required "
                f"username='{cc_conf['sasl.username']}' "
                f"password='{cc_conf['sasl.password']}';"
            ),
        )
        .option("kafka.ssl.endpoint.identification.algorithm", "https")
        .option("kafka.sasl.mechanism", cc_conf["sasl.mechanisms"])
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
        .selectExpr("substring(value, 6) as avro_value")
        .select(
            from_avro(col("avro_value"), topic_schema[1]).alias("avro_value"),
        )
        .writeStream.format("delta")
        .outputMode("append")
        .trigger(
            processingTime=trigger["processingTime"],
            availableNow=trigger["availableNow"],
        )
        .option(
            "checkpointLocation",
            f"/user/hive/warehouse/{schema}/{table}/_checkpoints",
        )
        .queryName(f"{catalog}_{schema}_{table}")
        .toTable(f"{catalog}.{schema}.{table}")
    )