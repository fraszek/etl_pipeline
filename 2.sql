-- Task 2.3: Count canceled trains within a given timestamp
-- Note: Each canceled stop counts as one cancellation. 
--       If a train has two stops within the timestamp and both are canceled, count as two.

-- Prepare the query with a timestamp parameter
PREPARE cancelled_trains(timestamp) AS
SELECT COUNT(*)
FROM fact_train_movements
WHERE
(
    (planned_arrival >= $1 AND planned_arrival < $1 + INTERVAL '1 hour')
 OR (planned_departure >= $1 AND planned_departure < $1 + INTERVAL '1 hour')
)
AND
(
    arrival_status = 'c'
 OR departure_status = 'c'
);

-- Example execution:
-- EXECUTE cancelled_trains('2025-09-25 11:00');



-- Task 2.4: Average train delay for a given station
-- Logic: Only arrival delays are considered. Arrivals on time and departure times are ignored.
--       AVG(actual_arrival - planned_arrival) gives the average delay for delayed arrivals.

PREPARE station_delay(text) AS
WITH arrivals AS (
    SELECT f.actual_arrival, f.planned_arrival
    FROM fact_train_movements f
    JOIN dim_stations s
      ON f.station_key = s.eva_id
    WHERE s.station_name = $1
      AND f.actual_arrival IS NOT NULL
      AND f.actual_arrival > f.planned_arrival
)
SELECT AVG(actual_arrival - planned_arrival) AS avg_delay
FROM arrivals;

-- Example execution:
-- EXECUTE station_delay('Berlin Hauptbahnhof');
