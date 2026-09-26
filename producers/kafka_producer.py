import json

from kafka import KafkaProducer
from kafka.serializer import Serializer


class JsonValueSerializer(Serializer):
    def serialize(self, topic, value):
        return json.dumps(value, default=str).encode("utf-8")


KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "vitals.events"


def create_producer():
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=JsonValueSerializer(),
    )


def publish_event(producer, event):
    return producer.send(
        KAFKA_TOPIC,
        key=event["patient_id"].encode("utf-8"),
        value=event,
    )