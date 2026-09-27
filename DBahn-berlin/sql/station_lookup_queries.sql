-- =============================================================================
-- Station lookup queries (Task 2.1 and Task 2.2)
-- -----------------------------------------------------------------------------
-- Both queries run against the station dimension table of the star schema:
--     dim_stations(eva_id, name, latitude, longitude, ...)
--
-- They are written as PostgreSQL prepared statements. Run PREPARE once per
-- session, then call EXECUTE with your own parameters (examples below each query).
-- =============================================================================


-- -----------------------------------------------------------------------------
-- Task 2.1: Given a station name, return its coordinates and identifier.
--   $1 (text) : exact station name, e.g. 'Ahrensfelde'
--   Returns   : longitude, latitude and EVA number (DB station identifier)
-- -----------------------------------------------------------------------------
PREPARE simple_select(text) AS

SELECT longitude, latitude, eva_id
FROM dim_stations WHERE dim_stations.name = $1

-- Example:
--EXECUTE simple_select('Ahrensfelde')


-- -----------------------------------------------------------------------------
-- Task 2.2: Given latitude/longitude, return the name of the closest station.
--   $1 (double precision) : latitude
--   $2 (double precision) : longitude
--   Returns               : the name of the nearest station
--
-- Distance is the squared Euclidean distance in degrees. Only the order of the
-- distances matters, so the square root is skipped. Over an area the size of
-- Berlin this is close enough to the true distance to find the nearest station.
-- -----------------------------------------------------------------------------
PREPARE closest_station(double precision, double precision) AS
SELECT
    name,
    POWER($1 - latitude, 2) +
    POWER($2 - longitude, 2) AS distance_squared
FROM dim_stations
ORDER BY distance_squared
LIMIT 1;

-- Example:
--EXECUTE closest_station(52.5692, 13.6081);
