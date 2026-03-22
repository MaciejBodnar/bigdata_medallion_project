DROP TABLE IF EXISTS silver.taxi_trips_cleaned;

CREATE TABLE silver.taxi_trips_cleaned AS
WITH
    yellow
    AS
    (
        SELECT
            'yellow' AS service_type,
            TRY_CAST(VendorID AS INTEGER) AS vendor_id,
            TRY_CAST(tpep_pickup_datetime AS TIMESTAMP) AS pickup_datetime,
            TRY_CAST(tpep_dropoff_datetime AS TIMESTAMP) AS dropoff_datetime,
            TRY_CAST(passenger_count AS INTEGER) AS passenger_count,
            TRY_CAST(trip_distance AS DOUBLE) AS trip_distance,
            TRY_CAST(RatecodeID AS INTEGER) AS rate_code_id,
            TRY_CAST(store_and_fwd_flag AS VARCHAR) AS store_and_fwd_flag,
            TRY_CAST(PULocationID AS INTEGER) AS pu_location_id,
            TRY_CAST(DOLocationID AS INTEGER) AS do_location_id,
            TRY_CAST(payment_type AS INTEGER) AS payment_type,
            TRY_CAST(fare_amount AS DOUBLE) AS fare_amount,
            TRY_CAST(extra AS DOUBLE) AS extra,
            TRY_CAST(mta_tax AS DOUBLE) AS mta_tax,
            TRY_CAST(tip_amount AS DOUBLE) AS tip_amount,
            TRY_CAST(tolls_amount AS DOUBLE) AS tolls_amount,
            TRY_CAST(improvement_surcharge AS DOUBLE) AS improvement_surcharge,
            TRY_CAST(total_amount AS DOUBLE) AS total_amount,
            TRY_CAST(congestion_surcharge AS DOUBLE) AS congestion_surcharge,
            TRY_CAST(Airport_fee AS DOUBLE) AS airport_fee
        FROM raw.yellow_taxi_trips
    ),
    green
    AS
    (
        SELECT
            'green' AS service_type,
            TRY_CAST(VendorID AS INTEGER) AS vendor_id,
            TRY_CAST(lpep_pickup_datetime AS TIMESTAMP) AS pickup_datetime,
            TRY_CAST(lpep_dropoff_datetime AS TIMESTAMP) AS dropoff_datetime,
            TRY_CAST(passenger_count AS INTEGER) AS passenger_count,
            TRY_CAST(trip_distance AS DOUBLE) AS trip_distance,
            TRY_CAST(RatecodeID AS INTEGER) AS rate_code_id,
            TRY_CAST(store_and_fwd_flag AS VARCHAR) AS store_and_fwd_flag,
            TRY_CAST(PULocationID AS INTEGER) AS pu_location_id,
            TRY_CAST(DOLocationID AS INTEGER) AS do_location_id,
            TRY_CAST(payment_type AS INTEGER) AS payment_type,
            TRY_CAST(fare_amount AS DOUBLE) AS fare_amount,
            TRY_CAST(extra AS DOUBLE) AS extra,
            TRY_CAST(mta_tax AS DOUBLE) AS mta_tax,
            TRY_CAST(tip_amount AS DOUBLE) AS tip_amount,
            TRY_CAST(tolls_amount AS DOUBLE) AS tolls_amount,
            TRY_CAST(improvement_surcharge AS DOUBLE) AS improvement_surcharge,
            TRY_CAST(total_amount AS DOUBLE) AS total_amount,
            TRY_CAST(congestion_surcharge AS DOUBLE) AS congestion_surcharge,
            CAST(NULL AS DOUBLE) AS airport_fee
        FROM raw.green_taxi_trips
    ),
    unioned
    AS
    (
                    SELECT *
            FROM yellow
        UNION ALL
            SELECT *
            FROM green
    ),
    filtered
    AS
    (
        SELECT
            service_type,
            vendor_id,
            pickup_datetime,
            dropoff_datetime,
            passenger_count,
            trip_distance,
            rate_code_id,
            store_and_fwd_flag,
            pu_location_id,
            do_location_id,
            payment_type,
            fare_amount,
            extra,
            mta_tax,
            tip_amount,
            tolls_amount,
            improvement_surcharge,
            total_amount,
            congestion_surcharge,
            airport_fee,
            DATE_TRUNC('month', pickup_datetime) AS pickup_month,
            DATE_DIFF('minute', pickup_datetime, dropoff_datetime) AS trip_duration_minutes
        FROM unioned
        WHERE pickup_datetime IS NOT NULL
            AND dropoff_datetime IS NOT NULL
            AND pickup_datetime <= dropoff_datetime
            AND trip_distance IS NOT NULL
            AND trip_distance >= 0
            AND total_amount IS NOT NULL
            AND total_amount > 0
            AND pu_location_id IS NOT NULL
            AND do_location_id IS NOT NULL
    )
SELECT *
FROM filtered
WHERE trip_duration_minutes BETWEEN 1 AND 720
    AND trip_distance <= 300
    AND total_amount <= 1000;
