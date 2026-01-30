
--2.1 Change the station name in the params CTE as needed
PREPARE simple_select(text) AS

SELECT longitude, latitude, eva_id 
FROM dim_stations WHERE dim_stations.name = $1

--EXECUTE simple_select('Ahrensfelde')

--2.2 Change the latitude and longitude values in the params CTE as needed

PREPARE closest_station(double precision, double precision) AS
SELECT
    name,
    POWER($1 - latitude, 2) +
    POWER($2 - longitude, 2) AS distance_squared
FROM dim_stations
ORDER BY distance_squared
LIMIT 1;

--EXECUTE closest_station(52.5692, 13.6081);