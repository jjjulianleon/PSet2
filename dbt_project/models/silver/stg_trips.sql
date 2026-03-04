SELECT
    "VendorID" AS vendor_id,
    pickup_datetime AS pickup_ts,
    dropoff_datetime AS dropoff_ts,
    "PULocationID" AS pu_location_id,
    "DOLocationID" AS do_location_id,
    passenger_count,
    trip_distance,
    fare_amount,
    extra,
    mta_tax,
    tip_amount,
    tolls_amount,
    improvement_surcharge,
    total_amount,
    payment_type,
    congestion_surcharge,
    airport_fee,
    trip_type,
    service_type,
    source_month,
    ingest_ts
FROM {{ source('bronze', 'trips') }}
WHERE
    pickup_datetime IS NOT NULL
    AND dropoff_datetime IS NOT NULL
    AND pickup_datetime <= dropoff_datetime
    AND trip_distance >= 0
    AND total_amount >= 0
    AND pickup_datetime >= '2022-01-01'
    AND pickup_datetime < '2026-01-01'
