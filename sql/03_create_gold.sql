DROP TABLE IF EXISTS gold.monthly_zone_metrics;

CREATE TABLE gold.monthly_zone_metrics AS
SELECT
    s.pickup_month,
    s.service_type,
    s.pu_location_id,
    COALESCE(z.Borough, 'Unknown') AS borough,
    COALESCE(z.Zone, 'Unknown') AS zone,
    COUNT(*) AS trip_count,
    SUM(s.total_amount) AS total_revenue,
    AVG(s.total_amount) AS avg_trip_revenue,
    AVG(s.trip_distance) AS avg_trip_distance,
    AVG(s.trip_duration_minutes) AS avg_trip_duration_minutes,
    SUM(COALESCE(s.passenger_count, 0)) AS total_passengers
FROM silver.taxi_trips_cleaned s
LEFT JOIN raw.taxi_zone_lookup z
    ON s.pu_location_id = TRY_CAST(z.LocationID AS INTEGER)
GROUP BY 1, 2, 3, 4, 5;

DROP TABLE IF EXISTS gold.payment_type_metrics;

CREATE TABLE gold.payment_type_metrics AS
SELECT
    pickup_month,
    service_type,
    payment_type,
    COUNT(*) AS trip_count,
    SUM(total_amount) AS total_revenue,
    AVG(total_amount) AS avg_trip_revenue
FROM silver.taxi_trips_cleaned
GROUP BY 1, 2, 3;
