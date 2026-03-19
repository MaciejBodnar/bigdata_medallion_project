-- Row counts
    SELECT 'raw.yellow_taxi_trips' AS table_name, COUNT(*) AS row_count
    FROM raw.yellow_taxi_trips
UNION ALL
    SELECT 'raw.green_taxi_trips', COUNT(*)
    FROM raw.green_taxi_trips
UNION ALL
    SELECT 'silver.taxi_trips_cleaned', COUNT(*)
    FROM silver.taxi_trips_cleaned
UNION ALL
    SELECT 'gold.monthly_zone_metrics', COUNT(*)
    FROM gold.monthly_zone_metrics;

-- Data quality check: unmatched zones
SELECT
    COUNT(*) AS unmatched_zone_rows
FROM gold.monthly_zone_metrics
WHERE zone = 'Unknown';

-- Top 20 zone-month combinations by revenue
SELECT *
FROM gold.monthly_zone_metrics
ORDER BY total_revenue DESC
LIMIT 20;
