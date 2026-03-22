import argparse
import shutil
import ssl
import time
from pathlib import Path
from urllib.error import URLError
from urllib.request import Request, urlopen

BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"
ZONE_URL = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"


def _build_ssl_context() -> ssl.SSLContext:
    """Create an SSL context with a reliable CA bundle when available."""
    try:
        from pip._vendor import certifi as pip_certifi
        return ssl.create_default_context(cafile=pip_certifi.where())
    except Exception:
        return ssl.create_default_context()


SSL_CONTEXT = _build_ssl_context()


def download(url: str, dest: Path, retries: int = 3) -> None:
    dest.parent.mkdir(parents=True, exist_ok=True)
    if dest.exists():
        print(f"[skip] {dest}")
        return

    last_error: Exception | None = None
    attempts_made = 0
    for attempt in range(1, retries + 1):
        attempts_made = attempt
        try:
            print(f"[download] {url} -> {dest}")
            request = Request(url, headers={"User-Agent": "medallion-pipeline/1.0"})
            with urlopen(request, context=SSL_CONTEXT, timeout=60) as response:
                with dest.open("wb") as output_file:
                    shutil.copyfileobj(response, output_file)
            return
        except Exception as exc:
            last_error = exc

            if dest.exists():
                dest.unlink(missing_ok=True)
            if attempt < retries:
                wait_seconds = attempt
                print(
                    f"[warn] download failed (attempt {attempt}/{retries}) for {url}: {exc}. "
                    f"retrying in {wait_seconds}s"
                )
                time.sleep(wait_seconds)

    raise RuntimeError(
        f"failed to download after {attempts_made} attempt(s): {url}"
    ) from last_error


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--years", nargs="+", type=int, required=True)
    parser.add_argument("--services", nargs="+", choices=["yellow", "green"], required=True)
    parser.add_argument("--output-dir", default="data/raw")
    args = parser.parse_args()

    output_dir = Path(args.output_dir)

    for service in args.services:
        for year in args.years:
            for month in range(1, 13):
                file_name = f"{service}_tripdata_{year}-{month:02d}.parquet"
                url = f"{BASE_URL}/{file_name}"
                dest = output_dir / service / file_name
                try:
                    download(url, dest)
                except Exception as exc:
                    print(f"[warn] failed to download {url}: {exc}")

    try:
        download(ZONE_URL, output_dir / "taxi_zone_lookup.csv")
    except (URLError, RuntimeError) as exc:
        raise SystemExit(
            "[error] failed to download taxi zone lookup. "
            "If you are on macOS and use python.org Python, run Install Certificates.command "
            "or install certifi in your active environment. "
            f"Details: {exc}"
        )


if __name__ == "__main__":
    main()
