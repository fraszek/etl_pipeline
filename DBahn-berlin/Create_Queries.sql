CREATE TABLE IF NOT EXISTS public.stations
(
    name character varying(255) COLLATE pg_catalog."default",
    evanumber integer NOT NULL,
    evanumber_geographiccoordinates_longitude numeric(10,7),
    evanumber_geographiccoordinates_latitude numeric(10,7),
    
    CONSTRAINT stations_pkey PRIMARY KEY (evanumber)
)