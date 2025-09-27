CREATE TABLE date_dimension (
    tripID INT PRIMARY KEY,
    pickup_year INT,
    pickup_month INT,
    pickup_day INT,
    dropoff_year INT,
    dropoff_month INT,
    dropoff_day INT,
    trip_hours DECIMAL(6,2)
    CONSTRAINT fk_trip
        FOREIGN KEY (tripID)
        REFERENCES denormalized_taxi(tripID)
);
INSERT INTO date_dimension
SELECT
    tripID,
    YEAR(STR_TO_DATE(tpep_pickup_datetime, '%c/%e/%Y %l:%i %p'))  AS pickup_year,
    MONTH(STR_TO_DATE(tpep_pickup_datetime, '%c/%e/%Y %l:%i %p')) AS pickup_month,
    DAY(STR_TO_DATE(tpep_pickup_datetime, '%c/%e/%Y %l:%i %p'))   AS pickup_day,
    YEAR(STR_TO_DATE(tpep_dropoff_datetime, '%c/%e/%Y %l:%i %p'))  AS dropoff_year,
    MONTH(STR_TO_DATE(tpep_dropoff_datetime, '%c/%e/%Y %l:%i %p')) AS dropoff_month,
    DAY(STR_TO_DATE(tpep_dropoff_datetime, '%c/%e/%Y %l:%i %p'))   AS dropoff_day,
    ROUND(
        TIMESTAMPDIFF(MINUTE,
            STR_TO_DATE(tpep_pickup_datetime, '%c/%e/%Y %l:%i %p'),
            STR_TO_DATE(tpep_dropoff_datetime, '%c/%e/%Y %l:%i %p')
        ) / 60, 2
    ) AS trip_hours
-- NOTE: This is in percentage to easy calculate number of hours :>
FROM denormalized_taxi;

SELECT * 
FROM date_dimension;
