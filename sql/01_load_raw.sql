-- This script expects local files under:
-- data/raw/yellow/*.parquet
-- data/raw/green/*.parquet
-- data/raw/taxi_zone_lookup.csv

DROP TABLE IF EXISTS raw.yellow_taxi_trips;
CREATE TABLE raw.yellow_taxi_trips AS
SELECT *
FROM read_parquet('data/raw/yellow/*.parquet', union_by_name = true);

DROP TABLE IF EXISTS raw.green_taxi_trips;
CREATE TABLE raw.green_taxi_trips AS
SELECT *
FROM read_parquet('data/raw/green/*.parquet', union_by_name = true);

DROP TABLE IF EXISTS raw.taxi_zone_lookup;
CREATE TABLE raw.taxi_zone_lookup AS
SELECT *
FROM read_csv_auto('data/raw/taxi_zone_lookup.csv', header = true);
