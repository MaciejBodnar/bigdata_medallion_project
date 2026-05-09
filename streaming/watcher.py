from __future__ import annotations

import json
import logging
import re
import time
from pathlib import Path

from streaming.config import StreamingConfig
from streaming.message_schema import RawFileEvent
from streaming.publisher import RabbitMQPublisher


logger = logging.getLogger(__name__)
FILE_NAME_PATTERN = re.compile(r"^(yellow|green)_tripdata_(\d{4})-(\d{2})\.parquet$")


def _load_registry(path: Path) -> set[str]:
    if not path.exists():
        return set()
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
        if isinstance(payload, list):
            return {str(item) for item in payload}
    except Exception as exc:
        logger.warning("Could not load watcher registry (%s). Starting fresh.", exc)
    return set()


def _save_registry(path: Path, entries: set[str]) -> None:
    path.write_text(json.dumps(sorted(entries), indent=2), encoding="utf-8")


def _build_event_from_path(config: StreamingConfig, file_path: Path) -> RawFileEvent:
    match = FILE_NAME_PATTERN.match(file_path.name)
    if not match:
        raise ValueError(
            "Unsupported file name format. Expected <service>_tripdata_YYYY-MM.parquet."
        )

    service_type = match.group(1)
    year = int(match.group(2))
    month = int(match.group(3))

    relative_file_path = str(file_path.resolve().relative_to(config.project_root))
    return RawFileEvent(
        event_type="new_raw_file",
        service_type=service_type,
        year=year,
        month=month,
        file_path=relative_file_path,
    )


def run_watcher(config: StreamingConfig | None = None) -> None:
    runtime_config = config or StreamingConfig.create()
    runtime_config.ensure_runtime_dirs()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s - %(message)s")

    published_paths = _load_registry(runtime_config.processed_registry_path)
    logger.info("Watcher started. Monitoring: %s", runtime_config.incoming_dir)

    publisher = RabbitMQPublisher(runtime_config)
    publisher.connect()

    try:
        while True:
            for file_path in sorted(runtime_config.incoming_dir.glob("*.parquet")):
                absolute_path = str(file_path.resolve())
                if absolute_path in published_paths:
                    continue

                try:
                    event = _build_event_from_path(runtime_config, file_path)
                    publisher.publish_event(event)
                    published_paths.add(absolute_path)
                    _save_registry(runtime_config.processed_registry_path, published_paths)
                except Exception as exc:
                    logger.error("Failed to publish event for %s: %s", file_path, exc)

            time.sleep(runtime_config.poll_interval_seconds)
    except KeyboardInterrupt:
        logger.info("Watcher stopped by user.")
    finally:
        publisher.close()


def main() -> None:
    run_watcher()


if __name__ == "__main__":
    main()
