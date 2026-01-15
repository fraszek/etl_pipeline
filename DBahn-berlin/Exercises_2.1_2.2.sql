
--2.1 Change the station name in the params CTE as needed

WITH params AS (
    SELECT 'Ahrensfelde' as parname
)
SELECT evanumber_geographiccoordinates_longitude, evanumber_geographiccoordinates_latitude, evanumber 
FROM stations, params WHERE "name" = params.parname

--2.2 Change the latitude and longitude values in the params CTE as needed
SELECT "name", distance_squared from (

WITH params AS (
    SELECT 13.56 AS latitude, 52.5 AS longitude
)
SELECT "name",
    POWER(params.latitude - evanumber_geographiccoordinates_longitude, 2) 
    + POWER(params.longitude - evanumber_geographiccoordinates_latitude, 2) 
    AS distance_squared
FROM stations, params
) 
ORDER by distance_squared
LIMIT 1