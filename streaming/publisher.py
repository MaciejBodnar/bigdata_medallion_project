from __future__ import annotations

import logging

import pika

from streaming.config import StreamingConfig
from streaming.message_schema import RawFileEvent


logger = logging.getLogger(__name__)


class RabbitMQPublisher:
    def __init__(self, config: StreamingConfig) -> None:
        self._config = config
        self._connection: pika.BlockingConnection | None = None
        self._channel: pika.adapters.blocking_connection.BlockingChannel | None = None

    def connect(self) -> None:
        if self._connection and self._connection.is_open:
            return
        parameters = pika.ConnectionParameters(host=self._config.rabbitmq_host, port=self._config.rabbitmq_port)
        self._connection = pika.BlockingConnection(parameters)
        self._channel = self._connection.channel()
        self._channel.queue_declare(queue=self._config.rabbitmq_queue_name)
        logger.info("Connected to RabbitMQ at %s:%s", self._config.rabbitmq_host, self._config.rabbitmq_port)

    def close(self) -> None:
        if self._connection and self._connection.is_open:
            self._connection.close()

    def publish_event(self, event: RawFileEvent) -> None:
        if not self._channel or self._channel.is_closed:
            raise RuntimeError("Publisher channel is not initialized. Call connect() first.")

        body = event.to_json().encode("utf-8")
        self._channel.basic_publish(
            exchange="",
            routing_key=self._config.rabbitmq_queue_name,
            body=body,
            properties=pika.BasicProperties(content_type="application/json"),
        )
        logger.info("Published event for file: %s", event.file_path)
