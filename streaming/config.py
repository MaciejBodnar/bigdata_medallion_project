from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class StreamingConfig:
    project_root: Path
    rabbitmq_host: str
    rabbitmq_port: int
    rabbitmq_queue_name: str
    incoming_dir: Path
    poll_interval_seconds: int
    processed_registry_path: Path

    @classmethod
    def create(
        cls,
        project_root: Path | None = None,
        rabbitmq_host: str = "localhost",
        rabbitmq_port: int = 5672,
        rabbitmq_queue_name: str = "taxi_raw_file_events",
        poll_interval_seconds: int = 5,
        incoming_dir: Path | None = None,
        processed_registry_path: Path | None = None,
    ) -> "StreamingConfig":
        resolved_project_root = (project_root or Path(__file__).resolve().parents[1]).resolve()
        resolved_incoming = (incoming_dir or (resolved_project_root / "data" / "raw" / "incoming")).resolve()
        resolved_registry = (
            processed_registry_path
            or (resolved_project_root / "artifacts" / "watcher_published_registry.json")
        ).resolve()

        return cls(
            project_root=resolved_project_root,
            rabbitmq_host=rabbitmq_host,
            rabbitmq_port=rabbitmq_port,
            rabbitmq_queue_name=rabbitmq_queue_name,
            incoming_dir=resolved_incoming,
            poll_interval_seconds=poll_interval_seconds,
            processed_registry_path=resolved_registry,
        )

    def ensure_runtime_dirs(self) -> None:
        self.incoming_dir.mkdir(parents=True, exist_ok=True)
        self.processed_registry_path.parent.mkdir(parents=True, exist_ok=True)
