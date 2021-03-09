import json
import logging

import click

from .logconfig import DEFAULT_LOG_FORMAT, logging_config
from .kafka_consumer import (
    kafka_consumer,
    {{cookiecutter.underscored.upper()}}_DEFAULT_KAFKA_BROKERS,
    normalize_bootstrap_server_spec,
)


@click.group()
@click.version_option()
@click.option(
    "--log-format",
    type=click.STRING,
    default=DEFAULT_LOG_FORMAT,
    help="Python logging format string",
)
@click.option(
    "--log-level", default="INFO", help="Python logging level", show_default=True
)
@click.option(
    "--log-file",
    help="Python log output file",
    type=click.Path(dir_okay=False, writable=True, resolve_path=True),
    default=None,
)
def cli(log_format, log_level, log_file):
    "{{ cookiecutter.description }}"

    logging_config(log_format, log_level, log_file)


@cli.command(name="consume")
@click.option(
    "-b",
    "--bootstrap-servers",
    "kafka_bootstrap_servers",
    help="list of Kafka broker host:port specs",
    type=click.STRING,
    multiple=True,
    default={{cookiecutter.underscored.upper()}}_DEFAULT_KAFKA_BROKERS,
    show_default=True,
)
@click.option(
    "-g",
    "--group-id",
    "kafka_group_id",
    help="Kafka consumer group id",
    type=click.STRING,
    default="{{cookiecutter.hyphenated}}-group",
    show_default=True,
)
@click.option(
    "--restart-offsets",
    "kafka_reset_offsets",
    is_flag=True,
    default=False,
    help="Rewind partition offsets to the beginning",
    show_default=True,
)
@click.argument("kafka_topic", envvar="KAFKA_TOPIC", type=click.STRING)
def consume(kafka_topic, kafka_bootstrap_servers, kafka_group_id, kafka_reset_offsets):
    consumer = kafka_consumer(
        kafka_topic, kafka_bootstrap_servers, kafka_group_id, kafka_reset_offsets
    )

    while True:
        msg = consumer.poll(1.0)

        if msg is None:
            continue
        if msg.error():
            print("Consumer error: {}".format(msg.error()))
            continue

        print(f"Received message on {msg.topic()}[{msg.partition()}:{msg.offset()}]")
        try:
            logging.info("Message value length: %s", len(msg.value()))
            logging.info("Message headers")
            for key, value in msg.headers():
                logging.info("\t%s: %s", key, value)
            msg_obj = json.loads(msg.value())
            logging.info("Successfully parsed JSON message: %s", msg.headers().get('message-uuid', '<no-message-uuid>'))
            sys.stdout.flush()

        except:
            logging.debug(
                "Failed to process message %s[%s:%s]",
                msg.topic(),
                msg.partition(),
                msg.offset(),
                exc_info=True,
            )
