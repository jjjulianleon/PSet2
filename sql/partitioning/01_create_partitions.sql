-- =============================================
-- PARTICIONAMIENTO DECLARATIVO EN POSTGRESQL
-- Ejecutar ANTES de dbt run --select gold
-- =============================================

-- =============================================
-- 1. fct_trips: RANGE por pickup_date (mensual)
-- =============================================
DROP TABLE IF EXISTS gold.fct_trips CASCADE;

CREATE TABLE gold.fct_trips (
    trip_key BIGINT,
    pickup_date_key INT,
    pickup_date DATE,
    pu_zone_key INT,
    do_zone_key INT,
    service_type_key INT,
    payment_type_key INT,
    vendor_key INT,
    trip_distance DOUBLE PRECISION,
    fare_amount DOUBLE PRECISION,
    tip_amount DOUBLE PRECISION,
    tolls_amount DOUBLE PRECISION,
    total_amount DOUBLE PRECISION,
    passenger_count DOUBLE PRECISION,
    duration_minutes DOUBLE PRECISION,
    pickup_hour INT
) PARTITION BY RANGE (pickup_date);

-- Particiones mensuales 2022
CREATE TABLE gold.fct_trips_2022_01 PARTITION OF gold.fct_trips FOR VALUES FROM ('2022-01-01') TO ('2022-02-01');
CREATE TABLE gold.fct_trips_2022_02 PARTITION OF gold.fct_trips FOR VALUES FROM ('2022-02-01') TO ('2022-03-01');
CREATE TABLE gold.fct_trips_2022_03 PARTITION OF gold.fct_trips FOR VALUES FROM ('2022-03-01') TO ('2022-04-01');
CREATE TABLE gold.fct_trips_2022_04 PARTITION OF gold.fct_trips FOR VALUES FROM ('2022-04-01') TO ('2022-05-01');
CREATE TABLE gold.fct_trips_2022_05 PARTITION OF gold.fct_trips FOR VALUES FROM ('2022-05-01') TO ('2022-06-01');
CREATE TABLE gold.fct_trips_2022_06 PARTITION OF gold.fct_trips FOR VALUES FROM ('2022-06-01') TO ('2022-07-01');
CREATE TABLE gold.fct_trips_2022_07 PARTITION OF gold.fct_trips FOR VALUES FROM ('2022-07-01') TO ('2022-08-01');
CREATE TABLE gold.fct_trips_2022_08 PARTITION OF gold.fct_trips FOR VALUES FROM ('2022-08-01') TO ('2022-09-01');
CREATE TABLE gold.fct_trips_2022_09 PARTITION OF gold.fct_trips FOR VALUES FROM ('2022-09-01') TO ('2022-10-01');
CREATE TABLE gold.fct_trips_2022_10 PARTITION OF gold.fct_trips FOR VALUES FROM ('2022-10-01') TO ('2022-11-01');
CREATE TABLE gold.fct_trips_2022_11 PARTITION OF gold.fct_trips FOR VALUES FROM ('2022-11-01') TO ('2022-12-01');
CREATE TABLE gold.fct_trips_2022_12 PARTITION OF gold.fct_trips FOR VALUES FROM ('2022-12-01') TO ('2023-01-01');

-- Particiones mensuales 2023
CREATE TABLE gold.fct_trips_2023_01 PARTITION OF gold.fct_trips FOR VALUES FROM ('2023-01-01') TO ('2023-02-01');
CREATE TABLE gold.fct_trips_2023_02 PARTITION OF gold.fct_trips FOR VALUES FROM ('2023-02-01') TO ('2023-03-01');
CREATE TABLE gold.fct_trips_2023_03 PARTITION OF gold.fct_trips FOR VALUES FROM ('2023-03-01') TO ('2023-04-01');
CREATE TABLE gold.fct_trips_2023_04 PARTITION OF gold.fct_trips FOR VALUES FROM ('2023-04-01') TO ('2023-05-01');
CREATE TABLE gold.fct_trips_2023_05 PARTITION OF gold.fct_trips FOR VALUES FROM ('2023-05-01') TO ('2023-06-01');
CREATE TABLE gold.fct_trips_2023_06 PARTITION OF gold.fct_trips FOR VALUES FROM ('2023-06-01') TO ('2023-07-01');
CREATE TABLE gold.fct_trips_2023_07 PARTITION OF gold.fct_trips FOR VALUES FROM ('2023-07-01') TO ('2023-08-01');
CREATE TABLE gold.fct_trips_2023_08 PARTITION OF gold.fct_trips FOR VALUES FROM ('2023-08-01') TO ('2023-09-01');
CREATE TABLE gold.fct_trips_2023_09 PARTITION OF gold.fct_trips FOR VALUES FROM ('2023-09-01') TO ('2023-10-01');
CREATE TABLE gold.fct_trips_2023_10 PARTITION OF gold.fct_trips FOR VALUES FROM ('2023-10-01') TO ('2023-11-01');
CREATE TABLE gold.fct_trips_2023_11 PARTITION OF gold.fct_trips FOR VALUES FROM ('2023-11-01') TO ('2023-12-01');
CREATE TABLE gold.fct_trips_2023_12 PARTITION OF gold.fct_trips FOR VALUES FROM ('2023-12-01') TO ('2024-01-01');

-- Particiones mensuales 2024
CREATE TABLE gold.fct_trips_2024_01 PARTITION OF gold.fct_trips FOR VALUES FROM ('2024-01-01') TO ('2024-02-01');
CREATE TABLE gold.fct_trips_2024_02 PARTITION OF gold.fct_trips FOR VALUES FROM ('2024-02-01') TO ('2024-03-01');
CREATE TABLE gold.fct_trips_2024_03 PARTITION OF gold.fct_trips FOR VALUES FROM ('2024-03-01') TO ('2024-04-01');
CREATE TABLE gold.fct_trips_2024_04 PARTITION OF gold.fct_trips FOR VALUES FROM ('2024-04-01') TO ('2024-05-01');
CREATE TABLE gold.fct_trips_2024_05 PARTITION OF gold.fct_trips FOR VALUES FROM ('2024-05-01') TO ('2024-06-01');
CREATE TABLE gold.fct_trips_2024_06 PARTITION OF gold.fct_trips FOR VALUES FROM ('2024-06-01') TO ('2024-07-01');
CREATE TABLE gold.fct_trips_2024_07 PARTITION OF gold.fct_trips FOR VALUES FROM ('2024-07-01') TO ('2024-08-01');
CREATE TABLE gold.fct_trips_2024_08 PARTITION OF gold.fct_trips FOR VALUES FROM ('2024-08-01') TO ('2024-09-01');
CREATE TABLE gold.fct_trips_2024_09 PARTITION OF gold.fct_trips FOR VALUES FROM ('2024-09-01') TO ('2024-10-01');
CREATE TABLE gold.fct_trips_2024_10 PARTITION OF gold.fct_trips FOR VALUES FROM ('2024-10-01') TO ('2024-11-01');
CREATE TABLE gold.fct_trips_2024_11 PARTITION OF gold.fct_trips FOR VALUES FROM ('2024-11-01') TO ('2024-12-01');
CREATE TABLE gold.fct_trips_2024_12 PARTITION OF gold.fct_trips FOR VALUES FROM ('2024-12-01') TO ('2025-01-01');

-- Particiones mensuales 2025
CREATE TABLE gold.fct_trips_2025_01 PARTITION OF gold.fct_trips FOR VALUES FROM ('2025-01-01') TO ('2025-02-01');
CREATE TABLE gold.fct_trips_2025_02 PARTITION OF gold.fct_trips FOR VALUES FROM ('2025-02-01') TO ('2025-03-01');
CREATE TABLE gold.fct_trips_2025_03 PARTITION OF gold.fct_trips FOR VALUES FROM ('2025-03-01') TO ('2025-04-01');
CREATE TABLE gold.fct_trips_2025_04 PARTITION OF gold.fct_trips FOR VALUES FROM ('2025-04-01') TO ('2025-05-01');
CREATE TABLE gold.fct_trips_2025_05 PARTITION OF gold.fct_trips FOR VALUES FROM ('2025-05-01') TO ('2025-06-01');
CREATE TABLE gold.fct_trips_2025_06 PARTITION OF gold.fct_trips FOR VALUES FROM ('2025-06-01') TO ('2025-07-01');
CREATE TABLE gold.fct_trips_2025_07 PARTITION OF gold.fct_trips FOR VALUES FROM ('2025-07-01') TO ('2025-08-01');
CREATE TABLE gold.fct_trips_2025_08 PARTITION OF gold.fct_trips FOR VALUES FROM ('2025-08-01') TO ('2025-09-01');
CREATE TABLE gold.fct_trips_2025_09 PARTITION OF gold.fct_trips FOR VALUES FROM ('2025-09-01') TO ('2025-10-01');
CREATE TABLE gold.fct_trips_2025_10 PARTITION OF gold.fct_trips FOR VALUES FROM ('2025-10-01') TO ('2025-11-01');
CREATE TABLE gold.fct_trips_2025_11 PARTITION OF gold.fct_trips FOR VALUES FROM ('2025-11-01') TO ('2025-12-01');
CREATE TABLE gold.fct_trips_2025_12 PARTITION OF gold.fct_trips FOR VALUES FROM ('2025-12-01') TO ('2026-01-01');

-- Particion default para fechas fuera de rango
CREATE TABLE gold.fct_trips_default PARTITION OF gold.fct_trips DEFAULT;

-- Insertar datos desde silver
INSERT INTO gold.fct_trips
SELECT
    ROW_NUMBER() OVER () AS trip_key,
    TO_CHAR(pickup_ts::date, 'YYYYMMDD')::int AS pickup_date_key,
    pickup_ts::date AS pickup_date,
    pu_location_id AS pu_zone_key,
    do_location_id AS do_zone_key,
    CASE service_type WHEN 'yellow' THEN 1 WHEN 'green' THEN 2 END AS service_type_key,
    payment_type::int AS payment_type_key,
    vendor_id::int AS vendor_key,
    trip_distance,
    fare_amount,
    tip_amount,
    tolls_amount,
    total_amount,
    passenger_count,
    EXTRACT(EPOCH FROM (dropoff_ts - pickup_ts)) / 60.0 AS duration_minutes,
    EXTRACT(HOUR FROM pickup_ts)::int AS pickup_hour
FROM silver.stg_trips;


-- =============================================
-- 2. dim_zone: HASH por zone_key (4 particiones)
-- =============================================
DROP TABLE IF EXISTS gold.dim_zone CASCADE;

CREATE TABLE gold.dim_zone (
    zone_key INT,
    zone_name TEXT,
    borough TEXT,
    service_zone TEXT
) PARTITION BY HASH (zone_key);

CREATE TABLE gold.dim_zone_p0 PARTITION OF gold.dim_zone FOR VALUES WITH (MODULUS 4, REMAINDER 0);
CREATE TABLE gold.dim_zone_p1 PARTITION OF gold.dim_zone FOR VALUES WITH (MODULUS 4, REMAINDER 1);
CREATE TABLE gold.dim_zone_p2 PARTITION OF gold.dim_zone FOR VALUES WITH (MODULUS 4, REMAINDER 2);
CREATE TABLE gold.dim_zone_p3 PARTITION OF gold.dim_zone FOR VALUES WITH (MODULUS 4, REMAINDER 3);

-- Insertar datos
INSERT INTO gold.dim_zone
SELECT "LocationID" AS zone_key, "Zone" AS zone_name, "Borough" AS borough, service_zone
FROM bronze.taxi_zones;


-- =============================================
-- 3. dim_service_type: LIST por service_type
-- =============================================
DROP TABLE IF EXISTS gold.dim_service_type CASCADE;

CREATE TABLE gold.dim_service_type (
    service_type_key INT,
    service_type TEXT
) PARTITION BY LIST (service_type);

CREATE TABLE gold.dim_service_type_yellow PARTITION OF gold.dim_service_type FOR VALUES IN ('yellow');
CREATE TABLE gold.dim_service_type_green PARTITION OF gold.dim_service_type FOR VALUES IN ('green');

-- Insertar datos
INSERT INTO gold.dim_service_type VALUES (1, 'yellow'), (2, 'green');


-- =============================================
-- 4. dim_payment_type: LIST por payment_type
-- =============================================
DROP TABLE IF EXISTS gold.dim_payment_type CASCADE;

CREATE TABLE gold.dim_payment_type (
    payment_type_key INT,
    payment_type TEXT
) PARTITION BY LIST (payment_type);

CREATE TABLE gold.dim_payment_type_credit PARTITION OF gold.dim_payment_type FOR VALUES IN ('Credit card');
CREATE TABLE gold.dim_payment_type_cash PARTITION OF gold.dim_payment_type FOR VALUES IN ('Cash');
CREATE TABLE gold.dim_payment_type_no_charge PARTITION OF gold.dim_payment_type FOR VALUES IN ('No charge');
CREATE TABLE gold.dim_payment_type_dispute PARTITION OF gold.dim_payment_type FOR VALUES IN ('Dispute');
CREATE TABLE gold.dim_payment_type_unknown PARTITION OF gold.dim_payment_type FOR VALUES IN ('Unknown');
CREATE TABLE gold.dim_payment_type_voided PARTITION OF gold.dim_payment_type FOR VALUES IN ('Voided trip');

-- Insertar datos
INSERT INTO gold.dim_payment_type VALUES
    (1, 'Credit card'), (2, 'Cash'), (3, 'No charge'),
    (4, 'Dispute'), (5, 'Unknown'), (6, 'Voided trip');
