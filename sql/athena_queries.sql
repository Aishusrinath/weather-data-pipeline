-- ============================================================
-- Weather Data Pipeline
-- Amazon Athena Analytical Queries
-- Database: weather_data_catalog
-- Table: processed
-- ============================================================


-- 1. Preview processed weather data
SELECT *
FROM weather_data_catalog.processed
LIMIT 10;


-- 2. Query a specific S3 partition
-- Partition filtering reduces unnecessary data scanning
-- as the dataset grows.
SELECT
    city,
    temperature,
    humidity,
    weather,
    year,
    month,
    day
FROM weather_data_catalog.processed
WHERE year = '2026'
  AND month = '09';


-- 3. Weather statistics by city
SELECT
    city,
    ROUND(AVG(temperature), 2) AS avg_temperature,
    ROUND(AVG(humidity), 2) AS avg_humidity,
    COUNT(*) AS observations
FROM weather_data_catalog.processed
GROUP BY city
ORDER BY city;


-- 4. Convert ISO-8601 processing time to an Athena timestamp
SELECT
    city,
    processed_at,
    from_iso8601_timestamp(processed_at) AS processed_timestamp
FROM weather_data_catalog.processed
ORDER BY from_iso8601_timestamp(processed_at);


-- 5. Daily weather statistics
SELECT
    year,
    month,
    day,
    ROUND(AVG(temperature), 2) AS avg_temperature,
    ROUND(AVG(humidity), 2) AS avg_humidity,
    COUNT(*) AS observations
FROM weather_data_catalog.processed
GROUP BY year, month, day
ORDER BY year, month, day;