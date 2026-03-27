CREATE DATABASE IF NOT EXISTS ridewave_raw_maheswar;


CREATE EXTERNAL TABLE ridewave_raw_maheswar.rides (
    ride_id      STRING,
    driver_id    STRING,
    customer_id  STRING,
    vehicle_id   STRING,
    city         STRING,
    fare_amount  DOUBLE,
    distance_km  DOUBLE,
    ride_status  STRING,
    ride_date    STRING,
    pickup_time  STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 's3://s3-de-q1-26/DE-Training/Day10/maheswar/raw/rides/'
TBLPROPERTIES ('skip.header.line.count'='1');

CREATE EXTERNAL TABLE ridewave_raw_maheswar.drivers (
    driver_id    STRING,
    driver_name  STRING,
    city         STRING,
    vehicle_type STRING,
    rating       DOUBLE,
    total_rides  INT,
    joined_date  STRING,
    is_active    STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 's3://s3-de-q1-26/DE-Training/Day10/maheswar/raw/drivers/'
TBLPROPERTIES ('skip.header.line.count'='1');


SELECT
    city,
    COUNT(*)                   AS total_rides,
    ROUND(SUM(fare_amount), 2) AS total_revenue
FROM ridewave_raw_maheswar.rides
WHERE fare_amount IS NOT NULL
GROUP BY city
ORDER BY total_rides DESC;

SELECT
    ride_status,
    COUNT(*) AS count
FROM ridewave_raw_maheswar.rides
GROUP BY ride_status
ORDER BY count DESC;


SELECT
    driver_id,
    COUNT(*)                   AS total_rides,
    ROUND(SUM(fare_amount), 2) AS total_earned
FROM ridewave_raw_maheswar.rides
WHERE fare_amount IS NOT NULL
GROUP BY driver_id
ORDER BY total_earned DESC
LIMIT 5;


SELECT
    driver_id,
    city,
    ROUND(SUM(fare_amount), 2) AS total_fare,
    RANK() OVER (
        PARTITION BY city
        ORDER BY SUM(fare_amount) DESC
    ) AS city_rank
FROM ridewave_raw_maheswar.rides
WHERE fare_amount IS NOT NULL
GROUP BY driver_id, city;


WITH monthly AS (
    SELECT
        DATE_TRUNC('month', CAST(ride_date AS DATE)) AS month,
        COUNT(*) AS ride_count
    FROM ridewave_raw_maheswar.rides
    GROUP BY 1
)
SELECT
    month,
    ride_count,
    LAG(ride_count) OVER (ORDER BY month) AS prev_month_count,
    ride_count - LAG(ride_count)
        OVER (ORDER BY month)              AS change
FROM monthly
ORDER BY month;

WITH completed_counts AS (
    SELECT driver_id, COUNT(*) AS total_rides
    FROM ridewave_raw_maheswar.rides
    GROUP BY driver_id
    HAVING COUNT(*) > 5
),
completed_status AS (
    SELECT DISTINCT driver_id
    FROM ridewave_raw_maheswar.rides
    WHERE ride_status = 'completed'
)
SELECT
    cc.driver_id,
    cc.total_rides,
    CASE WHEN cs.driver_id IS NOT NULL
         THEN 1 ELSE 0 END AS has_completed
FROM completed_counts cc
LEFT JOIN completed_status cs ON cc.driver_id = cs.driver_id
ORDER BY cc.total_rides DESC;