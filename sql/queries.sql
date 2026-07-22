-- Average temperature
SELECT city, AVG(temperature)
FROM weather_data
GROUP BY city;

-- Window function example
SELECT *,
       ROW_NUMBER() OVER (PARTITION BY city ORDER BY temperature DESC) as rank
FROM weather_data;