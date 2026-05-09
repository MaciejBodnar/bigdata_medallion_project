from __future__ import annotations

import logging
import shutil
from pathlib import Path

import pika

from pipelines.taxi.ingestion import ZONE_URL, download
from pipelines.taxi.main import run_pipeline_for_scope
from streaming.config import StreamingConfig
from streaming.message_schema import RawFileEvent


logger = logging.getLogger(__name__)


def _resolve_event_path(project_root: Path, file_path: str) -> Path:
    raw_path = Path(file_path)
    if raw_path.is_absolute():
        return raw_path
    return (project_root / raw_path).resolve()


def _move_file_to_service_raw(config: StreamingConfig, event: RawFileEvent) -> None:
    source_path = _resolve_event_path(config.project_root, event.file_path)
    destination_dir = config.project_root / "data" / "raw" / event.service_type
    destination_dir.mkdir(parents=True, exist_ok=True)
    destination_path = destination_dir / source_path.name

    if source_path.exists():
        shutil.move(str(source_path), str(destination_path))
        logger.info("Moved file to raw service directory: %s", destination_path)
        return

    if destination_path.exists():
        logger.info("File already moved previously: %s", destination_path)
        return

    raise FileNotFoundError(f"Incoming file does not exist: {source_path}")


def _handle_event(config: StreamingConfig, event: RawFileEvent) -> None:
    _move_file_to_service_raw(config, event)
    zone_lookup_path = config.project_root / "data" / "raw" / "taxi_zone_lookup.csv"
    if not zone_lookup_path.exists():
        logger.info("Missing taxi zone lookup file. Downloading required master file.")
        download(ZONE_URL, zone_lookup_path)

    result = run_pipeline_for_scope(
        project_root=config.project_root,
        year=event.year,
        month=event.month,
        service_type=event.service_type,
        skip_download=True,
    )
    logger.info(
        "Pipeline finished for %s %s-%02d. Summary keys: %s",
        event.service_type,
        event.year,
        event.month,
        list(result.keys()),
    )


def run_consumer(config: StreamingConfig | None = None) -> None:
    runtime_config = config or StreamingConfig.create()
    runtime_config.ensure_runtime_dirs()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s - %(message)s")

    parameters = pika.ConnectionParameters(
        host=runtime_config.rabbitmq_host,
        port=runtime_config.rabbitmq_port,
    )
    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()
    channel.queue_declare(queue=runtime_config.rabbitmq_queue_name)

    logger.info("Consumer started. Waiting for events on queue: %s", runtime_config.rabbitmq_queue_name)

    def on_message(
        ch: pika.adapters.blocking_connection.BlockingChannel,
        method: pika.spec.Basic.Deliver,
        properties: pika.spec.BasicProperties,
        body: bytes,
    ) -> None:
        del properties
        try:
            event = RawFileEvent.from_json(body.decode("utf-8"))
            _handle_event(runtime_config, event)
            ch.basic_ack(delivery_tag=method.delivery_tag)
        except Exception as exc:
            logger.error("Failed to process event: %s", exc)
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(queue=runtime_config.rabbitmq_queue_name, on_message_callback=on_message)

    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        logger.info("Consumer stopped by user.")
    finally:
        if channel.is_open:
            channel.close()
        if connection.is_open:
            connection.close()


def main() -> None:
    run_consumer()


if __name__ == "__main__":
    main()
