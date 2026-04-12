from __future__ import annotations

import shutil
import ssl
import time
from pathlib import Path
from typing import Any
from urllib.request import Request, urlopen

from pipelines.taxi.config import TaxiPipelineConfig


BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"
ZONE_URL = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"


def _build_ssl_context() -> ssl.SSLContext:
    try:
        import certifi

        return ssl.create_default_context(cafile=certifi.where())
    except Exception:
        return ssl.create_default_context()


SSL_CONTEXT = _build_ssl_context()


def download(url: str, dest: Path, retries: int = 3) -> None:
    dest.parent.mkdir(parents=True, exist_ok=True)
    if dest.exists():
        print(f"[skip] {dest}")
        return

    last_error: Exception | None = None
    for attempt in range(1, retries + 1):
        try:
            print(f"[download] {url} -> {dest}")
            request = Request(url, headers={"User-Agent": "medallion-pipeline/1.0"})
            with urlopen(request, context=SSL_CONTEXT, timeout=60) as response:
                with dest.open("wb") as output_file:
                    shutil.copyfileobj(response, output_file)
            return
        except Exception as exc:
            last_error = exc
            dest.unlink(missing_ok=True)
            if attempt < retries:
                time.sleep(attempt)

    raise RuntimeError(f"failed to download after {retries} attempt(s): {url}") from last_error


def download_source_files(config: TaxiPipelineConfig) -> dict[str, Any]:
    downloaded_files = 0
    skipped_files = 0
    failed_urls: list[str] = []

    for service in config.services:
        for year in config.years:
            for month in config.months:
                file_name = f"{service}_tripdata_{year}-{month:02d}.parquet"
                source_url = f"{BASE_URL}/{file_name}"
                destination_path = config.raw_dir / service / file_name

                already_exists = destination_path.exists()
                try:
                    download(source_url, destination_path)
                    if already_exists:
                        skipped_files += 1
                    else:
                        downloaded_files += 1
                except Exception:
                    failed_urls.append(source_url)

    zone_lookup_path = config.raw_dir / "taxi_zone_lookup.csv"
    zone_exists = zone_lookup_path.exists()
    download(ZONE_URL, zone_lookup_path)
    if zone_exists:
        skipped_files += 1
    else:
        downloaded_files += 1

    return {
        "downloaded_files": downloaded_files,
        "skipped_files": skipped_files,
        "failed_url_count": len(failed_urls),
        "failed_urls": failed_urls,
    }


def assert_minimum_input_files(config: TaxiPipelineConfig) -> None:
    missing_services: list[str] = []
    for service in config.services:
        pattern = "*.parquet"
        has_any_file = any((config.raw_dir / service).glob(pattern))
        if not has_any_file:
            missing_services.append(service)

    zone_lookup_path = config.raw_dir / "taxi_zone_lookup.csv"
    if missing_services:
        details = ", ".join(missing_services)
        raise FileNotFoundError(
            f"No parquet files found for services: {details}. Expected under {config.raw_dir}."
        )

    if not zone_lookup_path.exists():
        raise FileNotFoundError(f"Missing taxi zone lookup file: {zone_lookup_path}")
