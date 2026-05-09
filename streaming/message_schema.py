from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any


SUPPORTED_SERVICES = {"yellow", "green"}


@dataclass(frozen=True)
class RawFileEvent:
    event_type: str
    service_type: str
    year: int
    month: int
    file_path: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "event_type": self.event_type,
            "service_type": self.service_type,
            "year": self.year,
            "month": self.month,
            "file_path": self.file_path,
        }

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "RawFileEvent":
        event = cls(
            event_type=str(payload["event_type"]),
            service_type=str(payload["service_type"]),
            year=int(payload["year"]),
            month=int(payload["month"]),
            file_path=str(payload["file_path"]),
        )
        event.validate()
        return event

    def to_json(self) -> str:
        return json.dumps(self.to_dict())

    @classmethod
    def from_json(cls, raw: str) -> "RawFileEvent":
        payload = json.loads(raw)
        if not isinstance(payload, dict):
            raise ValueError("Message payload must be a JSON object")
        return cls.from_dict(payload)

    def validate(self) -> None:
        if self.event_type != "new_raw_file":
            raise ValueError(f"Unsupported event_type: {self.event_type}")
        if self.service_type not in SUPPORTED_SERVICES:
            raise ValueError(f"Unsupported service_type: {self.service_type}")
        if self.month < 1 or self.month > 12:
            raise ValueError(f"Invalid month: {self.month}")
        if self.year < 2000 or self.year > 2100:
            raise ValueError(f"Invalid year: {self.year}")
        if not self.file_path:
            raise ValueError("file_path cannot be empty")
