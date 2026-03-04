WITH date_spine AS (
    SELECT generate_series(
        '2022-01-01'::date,
        '2025-12-31'::date,
        '1 day'::interval
    )::date AS full_date
)
SELECT
    TO_CHAR(full_date, 'YYYYMMDD')::int AS date_key,
    full_date,
    EXTRACT(YEAR FROM full_date)::int AS year,
    EXTRACT(MONTH FROM full_date)::int AS month,
    EXTRACT(DAY FROM full_date)::int AS day,
    EXTRACT(DOW FROM full_date)::int AS day_of_week,
    TO_CHAR(full_date, 'Day') AS day_name,
    TO_CHAR(full_date, 'Month') AS month_name
FROM date_spine
