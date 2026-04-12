# NYC TLC Medallion Pipeline (Prefect + PySpark)

## Cel projektu

Repozytorium realizuje pipeline danych NYC TLC taxi w architekturze Medallion:

- bronze: surowe dane po inicjalnym załadowaniu,
- silver: dane ustandaryzowane i oczyszczone,
- gold: zagregowane tabele analityczne.

Aktualna, główna i jedyna wspierana ścieżka wykonania to **Prefect + PySpark**.

## Dlaczego Prefect + PySpark

- Prefect daje czytelny orchestration flow i taski uruchamiane lokalnie bez dodatkowej infrastruktury.
- PySpark zapewnia skalowalny silnik przetwarzania i łatwe przejście z local do większego środowiska.
- Zapis warstw jako Parquet upraszcza idempotentne ponowne uruchamianie i kontrolę pipeline.

## Sprawdzone wersje

- Prefect: `3.6.26`
- PySpark: `3.5.2`
- PyArrow: `20.0.0`

## Architektura

![Flow diagram](docs/flow-diagram.png)

Diagram pokazuje prosty przepływ: od pobrania danych, przez Bronze, Silver i Gold, aż do walidacji końcowej. To jest główna ścieżka działania projektu.

## Struktura repo

```text
.
├── pipelines/
│   └── taxi/
│       ├── config.py
│       ├── ingestion.py
│       ├── bronze.py
│       ├── silver.py
│       ├── gold.py
│       ├── validation.py
│       ├── spark_session.py
│       └── main.py
├── orchestration/
│   └── prefect_compat.py
├── docs/
│   ├── data_quality_risks.md
│   ├── flow-diagram.png
│   └── problem_statement.md
├── artifacts/                    # walidacja JSON (runtime)
├── data/
│   ├── raw/
│   ├── bronze/
│   ├── silver/
│   └── gold/
├── Makefile
└── requirements.txt
```

## Pojedynczy entrypoint

Główny entrypoint uruchamiający pełny przepływ:

```bash
python -m pipelines.taxi.main
```

## Instalacja

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Uruchomienie pipeline

### 1) Mała próbka (local smoke)

```bash
python -m pipelines.taxi.main --sample --years 2025 --months 1 --services yellow green
```

### 2) Pełniejszy zakres miesięcy

```bash
python -m pipelines.taxi.main --years 2024 2025 --months 1 2 3 4 5 6 7 8 9 10 11 12 --services yellow green
```

### 3) Użycie wcześniej pobranych danych (bez download)

```bash
python -m pipelines.taxi.main --skip-download --years 2025 --months 1 2 3 --services yellow green
```

## Co robi flow Prefect

Flow wykonuje kolejno taski:

1. prepare directories
2. download source files
3. build bronze layer
4. build silver layer
5. build gold layer
6. run data quality validation

## Warstwy danych

- data/raw: pobrane pliki NYC TLC (yellow/green parquet + taxi_zone_lookup.csv)
- data/bronze:
  - yellow_trips
  - green_trips
  - taxi_zone_lookup
- data/silver:
  - taxi_trips_cleaned
- data/gold:
  - monthly_zone_metrics
  - payment_type_metrics

## Idempotency

Idempotency jest zapewniona przez:

1. Downloader pomija istniejące pliki wejściowe.
2. Bronze buduje tylko zakres wskazany w argumentach (`years`, `months`, `services`) i nadpisuje wyłącznie odpowiadające partycje.
3. Silver i gold przeliczają tylko partycje/miesiące dotknięte w bieżącym przebiegu.
4. Pipeline nie usuwa globalnie danych ani folderów.
5. Wielokrotne uruchomienie z tym samym zakresem danych daje spójny wynik.

## Walidacja jakości

Po zbudowaniu gold uruchamiany jest moduł walidacji, który:

- liczy rekordy bronze/silver/gold,
- sprawdza reguły quality w silver,
- liczy liczbę rekordów gold z nierozpoznaną strefą,
- zapisuje raport do `artifacts/validation_report.json`,
- kończy flow błędem, jeśli krytyczne checki nie przejdą.

Reguły silver:

- pickup/dropoff not null,
- pickup <= dropoff,
- trip_distance >= 0 i <= 300,
- total_amount > 0 i <= 1000,
- pu/do location not null,
- duration between 1 and 720,
- deduplikacja logicznych duplikatów.

## CI smoke test

Workflow GitHub Actions tworzy małą próbkę danych i uruchamia nowy entrypoint Prefect + Spark.

## Szybkie komendy Makefile

```bash
make install
make run-sample
make run-full
make ci-smoke
```
