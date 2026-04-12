from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any


DEFAULT_SERVICES = ("yellow", "green")


@dataclass(frozen=True)
class TaxiPipelineConfig:
    project_root: Path
    raw_dir: Path
    bronze_dir: Path
    silver_dir: Path
    gold_dir: Path
    artifacts_dir: Path
    years: list[int]
    months: list[int]
    services: list[str]
    validation_report_path: Path
    run_download: bool = True

    @classmethod
    def create(
        cls,
        project_root: Path,
        years: list[int],
        months: list[int],
        services: list[str],
        run_download: bool,
        raw_dir: Path | None = None,
        bronze_dir: Path | None = None,
        silver_dir: Path | None = None,
        gold_dir: Path | None = None,
        artifacts_dir: Path | None = None,
        validation_report_path: Path | None = None,
    ) -> "TaxiPipelineConfig":
        normalized_project_root = project_root.resolve()
        resolved_raw = (raw_dir or normalized_project_root / "data" / "raw").resolve()
        resolved_bronze = (bronze_dir or normalized_project_root / "data" / "bronze").resolve()
        resolved_silver = (silver_dir or normalized_project_root / "data" / "silver").resolve()
        resolved_gold = (gold_dir or normalized_project_root / "data" / "gold").resolve()
        resolved_artifacts = (artifacts_dir or normalized_project_root / "artifacts").resolve()
        resolved_report = (
            validation_report_path
            or (resolved_artifacts / "validation_report.json")
        ).resolve()

        return cls(
            project_root=normalized_project_root,
            raw_dir=resolved_raw,
            bronze_dir=resolved_bronze,
            silver_dir=resolved_silver,
            gold_dir=resolved_gold,
            artifacts_dir=resolved_artifacts,
            years=sorted(set(years)),
            months=sorted(set(months)),
            services=sorted(set(services)),
            validation_report_path=resolved_report,
            run_download=run_download,
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "project_root": str(self.project_root),
            "raw_dir": str(self.raw_dir),
            "bronze_dir": str(self.bronze_dir),
            "silver_dir": str(self.silver_dir),
            "gold_dir": str(self.gold_dir),
            "artifacts_dir": str(self.artifacts_dir),
            "years": self.years,
            "months": self.months,
            "services": self.services,
            "validation_report_path": str(self.validation_report_path),
            "run_download": self.run_download,
        }

    @classmethod
    def from_dict(cls, raw: dict[str, Any]) -> "TaxiPipelineConfig":
        return cls(
            project_root=Path(raw["project_root"]),
            raw_dir=Path(raw["raw_dir"]),
            bronze_dir=Path(raw["bronze_dir"]),
            silver_dir=Path(raw["silver_dir"]),
            gold_dir=Path(raw["gold_dir"]),
            artifacts_dir=Path(raw["artifacts_dir"]),
            years=list(raw["years"]),
            months=list(raw["months"]),
            services=list(raw["services"]),
            validation_report_path=Path(raw["validation_report_path"]),
            run_download=bool(raw["run_download"]),
        )

    @property
    def bronze_taxi_root(self) -> Path:
        return self.bronze_dir / "taxi"

    @property
    def silver_taxi_root(self) -> Path:
        return self.silver_dir / "taxi"

    @property
    def gold_taxi_root(self) -> Path:
        return self.gold_dir / "taxi"

    @property
    def bronze_zone_lookup_path(self) -> str:
        return str(self.bronze_taxi_root / "taxi_zone_lookup")

    def bronze_service_month_root(self, service_type: str, year: int, month: int) -> Path:
        return self.bronze_taxi_root / f"service_type={service_type}" / f"pickup_year={year}" / f"pickup_month={month:02d}"

    def silver_month_root(self, service_type: str, year: int, month: int) -> Path:
        return self.silver_taxi_root / f"service_type={service_type}" / f"pickup_year={year}" / f"pickup_month={month:02d}"

    def silver_month_glob(self, year: int, month: int) -> str:
        return str(self.silver_taxi_root / "service_type=*" / f"pickup_year={year}" / f"pickup_month={month:02d}")

    @property
    def silver_trips_path(self) -> str:
        return str(self.silver_taxi_root)

    def gold_monthly_root(self, year: int, month: int) -> Path:
        return self.gold_taxi_root / "monthly_zone_metrics" / f"pickup_year={year}" / f"pickup_month={month:02d}"

    def gold_payment_root(self, year: int, month: int) -> Path:
        return self.gold_taxi_root / "payment_type_metrics" / f"pickup_year={year}" / f"pickup_month={month:02d}"

    @property
    def gold_monthly_metrics_path(self) -> str:
        return str(self.gold_taxi_root / "monthly_zone_metrics")

    @property
    def gold_payment_metrics_path(self) -> str:
        return str(self.gold_taxi_root / "payment_type_metrics")
