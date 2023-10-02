import configparser
from dataclasses import dataclass, field
from pathlib import Path

from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from utils.logger_setup import logger
from utils.source_strategy import SourceStrategy


@dataclass
class KafkaHelper:
    cp_conf: dict[str, str] = field(init=False)
    sr_conf: dict[str, str] = field(init=False)
    source_strategy: SourceStrategy

    def __post_init__(self) -> None:
        conf_parser: configparser.ConfigParser = configparser.ConfigParser()
        conf_parser.read(Path("config/conf.properties"))
        self.cp_conf = {
            "bootstrap.servers": conf_parser["CONFLUENT"]["bootstrap.servers"],
            "security.protocol": conf_parser["CONFLUENT"]["security.protocol"],
            "sasl.mechanisms": conf_parser["CONFLUENT"]["sasl.mechanisms"],
            "sasl.username": conf_parser["CONFLUENT"]["sasl.username"],
            "sasl.password": conf_parser["CONFLUENT"]["sasl.password"],
            "linger.ms": conf_parser["CONFLUENT"]["linger.ms"],
            "batch.num.messages": conf_parser["CONFLUENT"][
                "batch.num.messages"
            ],
            "queue.buffering.max.messages": conf_parser["CONFLUENT"][
                "queue.buffering.max.messages"
            ],
        }
        self.sr_conf = {
            "url": conf_parser["SCHEMA_REGISTRY"]["schema.registry.url"],
            "basic.auth.user.info": conf_parser["SCHEMA_REGISTRY"][
                "basic.auth.user.info"
            ],
        }
        logger.info("Configuration files were read succesfully.")

    def get_schemas(
        self,
        sr_client: SchemaRegistryClient,
        key_subject: str,
        value_subject: str,
    ) -> tuple[dict, dict]:
        key_schema: dict = sr_client.get_latest_version(
            key_subject
        ).schema.schema_str
        value_schema: dict = sr_client.get_latest_version(
            value_subject
        ).schema.schema_str
        logger.info("Key and value schemas were fetched.")
        return key_schema, value_schema

    def produce_source(self, topic: str, **kwargs) -> None:
        sr_client: SchemaRegistryClient = SchemaRegistryClient(self.sr_conf)
        cp_client: Producer = Producer(self.cp_conf)

        topic_schema = self.get_schemas(
            sr_client, f"{topic}-key", f"{topic}-value"
        )
        key_serializer: AvroSerializer = AvroSerializer(
            sr_client, topic_schema[0]
        )
        value_serializer: AvroSerializer = AvroSerializer(
            sr_client, topic_schema[1]
        )
        self.source_strategy.produce_source(
            cp_client, topic, key_serializer, value_serializer, kwargs
        )
        logger.info("Producer finalized succesfully.")
