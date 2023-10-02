import abc

from confluent_kafka import Producer


class SourceStrategy(abc.ABC):
    @abc.abstractclassmethod
    def produce_source(
        self,
        cp_client: Producer,
        topic: str,
        key_serializer: dict,
        value_serializer: dict,
        kwargs: dict,
    ) -> None:
        pass
