import abc

from confluent_kafka import Producer


class SourceStrategy(abc.ABC):
    """
    Abstract base class representing a strategy for producing messages to
    Kafka.
    """

    @abc.abstractclassmethod
    def produce_source(
        self,
        cp_client: Producer,
        topic: str,
        key_serializer: dict,
        value_serializer: dict,
        kwargs: dict,
    ) -> None:
        """
        Produce messages to the specified Kafka topic from a CSV file.

        Attributes:
            cp_client (Producer): Confluent Kafka producer client.
            topic (str): Kafka topic to produce messages to.
            key_serializer (str): Key serializer for message.
            value_serializer (str): Value serializer for message.
            kwargs (str): Additional keyword arguments.

        Returns:
            None
        """
        pass
