# Big Data – modelowanie, zarządzanie, przetwarzanie i integracja

## Temat projektu

**Analiza popytu i przychodów przejazdów taxi w Nowym Jorku w architekturze Medallion (Bronze / Silver / Gold)**

## Cel analityczny

Celem projektu jest zbudowanie prostego, ale realistycznego pipeline'u danych w architekturze medalionowej:

- **Bronze / raw** – surowe dane o kursach taxi i słowniki referencyjne,
- **Silver / cleaned** – ujednolicone i oczyszczone rekordy,
- **Gold / curated** – zagregowane dane biznesowe do analizy.

### Główne pytanie biznesowe

**Które strefy i miesiące generują największy popyt oraz przychód z przejazdów taxi?**

---

## Dlaczego ten dataset?

Oficjalne dane NYC TLC są publikowane **co miesiąc** i udostępniane w formacie **Parquet**, co dobrze pasuje do ćwiczenia big data i warstw bronze/silver/gold. Oficjalna strona NYC TLC podaje też, że dane są duże i mogą mieć drobne różnice schematów między latami, co jest dobrym, realistycznym przypadkiem jakości danych. citeturn100974search0turn100974search1turn100974search19

---

## Wybrana technologia

- **DuckDB** jako baza danych (plik `warehouse.duckdb`)
- **SQL** jako główny język transformacji
- **Parquet / CSV** jako format wejściowy
- **GitHub** jako repozytorium
- **GitHub Actions** do prostego CI

DuckDB pozwala bezpośrednio czytać pliki Parquet przez `read_parquet(...)`, także wiele plików naraz, a także tworzyć tabele przez `CREATE TABLE AS SELECT ...`. To upraszcza reproducibility i lokalne uruchomienie projektu. citeturn100974search3turn100974search13turn100974search19

---

## Struktura repo

```text
bigdata-medallion-nyc-taxi/
├── README.md
├── requirements.txt
├── .gitignore
├── sql/
│   ├── 00_init.sql
│   ├── 01_load_raw.sql
│   ├── 02_create_silver.sql
│   ├── 03_create_gold.sql
│   └── 04_validation_queries.sql
├── scripts/
│   ├── download_data.py
│   └── run_pipeline.py
├── docs/
│   ├── problem_statement.md
│   └── data_quality_risks.md
└── .github/
    └── workflows/
        └── ci.yml
```

---

## Dane wejściowe

### Pliki źródłowe

1. **Yellow Taxi Trip Records** – wiele miesięcznych plików parquet
2. **Green Taxi Trip Records** – wiele miesięcznych plików parquet
3. **Taxi Zone Lookup** – słownik stref

### Jak dojść do ~10 GB

Najprościej pobrać:

- **24–36 miesięcy Yellow Taxi**, oraz
- **24–36 miesięcy Green Taxi**

W praktyce warto pobrać więcej niż minimalne 10 GB, np. **ok. 12–15 GB**, żeby po kompresji / różnicach rozmiaru nie zejść poniżej wymagań.

Oficjalne źródło danych: NYC TLC Trip Record Data. Dane są publikowane co miesiąc jako pliki parquet. citeturn100974search0

---

## Model warstw

### 1. Bronze / raw

Schema: `raw`

Tabele:

- `raw.yellow_taxi_trips`
- `raw.green_taxi_trips`
- `raw.taxi_zone_lookup`

Charakterystyka:

- bez logiki biznesowej,
- minimalne castowanie,
- przechowanie danych „tak jak przyszły”.

### 2. Silver / cleaned

Schema: `silver`

Tabele:

- `silver.taxi_trips_cleaned`

Działania:

- ujednolicenie schematu yellow + green,
- odfiltrowanie rekordów błędnych,
- walidacja dat, dystansu i kwot,
- standaryzacja typów danych,
- deduplikacja.

### 3. Gold / curated

Schema: `gold`

Tabele:

- `gold.monthly_zone_metrics`
- `gold.payment_type_metrics`

Działania:

- JOIN do słownika stref,
- agregacja po miesiącu / strefie / typie płatności,
- metryki do analizy biznesowej.

---

## Jak uruchomić

### 1. Instalacja

```bash
python -m venv .venv
source .venv/bin/activate  # Linux/macOS
pip install -r requirements.txt
```

### 2. Pobranie danych

Przykład:

```bash
python scripts/download_data.py --years 2023 2024 2025 --services yellow green --output-dir data/raw
```

### 3. Uruchomienie pipeline'u

```bash
python scripts/run_pipeline.py --db warehouse.duckdb --data-dir data/raw
```

### 4. Odpalenie walidacji

```bash
duckdb warehouse.duckdb < sql/04_validation_queries.sql
```

---

## Co dokładnie spełnia wymagania z zadania?

- **Initial problem statement** → `docs/problem_statement.md`
- **Database created** → DuckDB database `warehouse.duckdb`
- **Raw table loaded** → `sql/01_load_raw.sql`
- **Cleaned table created** → `sql/02_create_silver.sql`
- **Gold layer with JOIN / aggregation** → `sql/03_create_gold.sql`
- **Reproducible SQL script** → skrypty SQL + `scripts/run_pipeline.py`
- **3 data quality risks** → `docs/data_quality_risks.md`
- **Git link** → repo na GitHub

---

## Proponowany opis do oddania / prezentacji

1. Postawiłem bazę danych DuckDB.
2. Załadowałem surowe dane NYC TLC do warstwy raw.
3. W silver ujednoliciłem schemat i oczyściłem rekordy.
4. W gold przygotowałem agregacje przychodów i liczby kursów per miesiąc / strefa.
5. Zidentyfikowałem 3 ryzyka jakości danych: braki referencyjne, błędne wartości liczbowe, niejednorodność schematu.
6. Całość jest odtwarzalna z repo i uruchamiana skryptami.

---

## Uwaga praktyczna

Do **CI** nie warto pobierać pełnych 10 GB. W CI uruchamiaj tylko mały sample danych, a pełny wsad buduj lokalnie. Dzięki temu repo pokaże poprawne praktyki inżynierskie bez ryzyka timeoutów.

---

## Dalsze rozszerzenia

- dashboard w Power BI / Metabase,
- partycjonowanie gold po miesiącu,
- testy jakości danych,
- eksport gold do parquet.
