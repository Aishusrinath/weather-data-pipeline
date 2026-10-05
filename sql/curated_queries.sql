-- ============================================================
-- Curated Weather Analytics Queries
-- Source: S3 curated layer (Snappy Parquet)
-- Catalog: weather_data_catalog
-- Table: curated
-- ============================================================


-- 1. View latest curated weather records
SELECT
    city,
    temperature,
    humidity,
    weather,
    processed_at,
    year,
    month,
    day
FROM weather_data_catalog.curated
ORDER BY processed_at DESC
LIMIT 10;


-- 2. Partition-filtered query
-- Demonstrates partition pruning using year/month/day.
SELECT
    city,
    temperature,
    humidity,
    weather,
    processed_at
FROM weather_data_catalog.curated
WHERE year = '2026'
  AND month = '09'
  AND day = '02';


-- 3. Aggregate weather metrics by city
SELECT
    city,
    ROUND(AVG(temperature), 2) AS avg_temperature,
    ROUND(AVG(humidity), 2) AS avg_humidity,
    COUNT(*) AS record_count
FROM weather_data_catalog.curated
GROUP BY city
ORDER BY city;