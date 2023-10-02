import csv
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from confluent_kafka import Producer
from confluent_kafka.serialization import MessageField, SerializationContext
from utils.logger_setup import logger
from utils.source_strategy import SourceStrategy


@dataclass
class CSVStrategy(SourceStrategy):
    @staticmethod
    def generate_msg_value(header: list, types: list, row: list) -> dict:
        ans: dict = {}
        for i in range(len(header)):
            ans[header[i]] = types[i](row[i])
        return ans

    @staticmethod
    def generate_msg_key(msg_value: dict, key: list) -> dict:
        ans: dict = {}
        for k in key:
            ans[k] = msg_value[k]
        return ans

    def produce_source(
        self,
        cp_client: Producer,
        topic: str,
        key_serializer: str,
        value_serializer: str,
        kwargs: str,
    ) -> None:
        with open(Path(kwargs["path"])) as csv_file:
            csv_reader: Any = csv.reader(csv_file, delimiter=",")
            next(csv_reader)
            count = 0
            for row in csv_reader:
                msg_value = self.generate_msg_value(
                    kwargs["header"], kwargs["types"], row
                )
                msg_key = self.generate_msg_key(msg_value, kwargs["key"])
                cp_client.produce(
                    topic=topic,
                    key=key_serializer(
                        msg_key,
                        SerializationContext(topic, MessageField.KEY),
                    ),
                    value=value_serializer(
                        msg_value,
                        SerializationContext(topic, MessageField.VALUE),
                    ),
                )
                count += 1
                if count % 10_000 == 0:
                    logger.info(
                        f"Number of sent messages is {count}. "
                        "Producer is flushing the queue."
                    )
                    cp_client.flush()
            logger.info(
                f"Number of sent messages is {count}. "
                "Producer is flushing the queue"
            )
            cp_client.flush()
