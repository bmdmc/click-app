import logging
import json
from pprint import pprint

import confluent_kafka
from confluent_kafka import Consumer, TopicPartition

{{cookiecutter.underscored.upper()}}_DEFAULT_KAFKA_BROKERS = [
    {% for broker in cookiecutter.brokers.split(",") %}
    "{{ broker }}",
    {% endfor %}
]


def normalize_bootstrap_server_spec(spec):
    """
    Add the default Kafka broker port to the spec if there isn't one
    since pykafka is finicky about that
    """

    if ":" not in spec:
        return spec + ":9092"
    else:
        return spec


def kafka_consumer(kafka_topic, bootstrap_servers, group_id, restart_offsets=False):
    logging.info(
        "Forwarding messages from Kafka topic %s, as Kafka group %s",
        kafka_topic,
        group_id,
    )

    clean_server_specs = ",".join(
        [normalize_bootstrap_server_spec(spec) for spec in bootstrap_servers]
    )

    c = Consumer(
        {
            "bootstrap.servers": clean_server_specs,
            "group.id": group_id,
            "auto.offset.reset": "earliest",
        }
    )

    try:
        logging.info("Subscribing to Kafka topic: %s", kafka_topic)

        c.subscribe([kafka_topic])
        topic_metadata = c.list_topics(kafka_topic)
        if topic_metadata.topics[kafka_topic].error is not None:
            raise confluent_kafka.KafkaException(
                topic_metadata.topics[kafka_topic].error
            )

        partitions = [
            confluent_kafka.TopicPartition(kafka_topic, p)
            for p in topic_metadata.topics[kafka_topic].partitions
        ]
        logging.info("Partitions: %s", partitions)

        if restart_offsets:

            c.assign(
                [
                    TopicPartition(
                        kafka_topic,
                        partition.partition,
                        confluent_kafka.OFFSET_BEGINNING,
                    )
                    for partition in partitions
                ]
            )

        logging.info("Connected!")
    except:
        logging.error(
            "Couldn't create Kafka consumer for topic: %s", kafka_topic, exc_info=True
        )

    return c
