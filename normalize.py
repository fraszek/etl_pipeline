import psycopg2

DB_PARAMS = {
    "host": "localhost",
    "port": 5432,
    "dbname": "postgres",
    "user": "postgres",
    "password": "postgres",
}

def main():
    conn = psycopg2.connect(**DB_PARAMS)
    cur = conn.cursor()

    cur.execute("ALTER TABLE dim_stations ADD COLUMN IF NOT EXISTS station_name_normalized TEXT;")
    cur.execute("ALTER TABLE staging_movements ADD COLUMN IF NOT EXISTS station_name_normalized TEXT;")

    # Normalize dim_stations
    cur.execute("""
        UPDATE dim_stations
        SET station_name_normalized =
                REPLACE(
                REPLACE(
                REPLACE(
                regexp_replace(
                REPLACE(
                REPLACE(
                REPLACE(
                REPLACE(
                    LOWER(station_name), 'berlin', ''
                ),
                    'pbf', ''
                ),
                    '-', ' '
                ),
                    '.', ' '
                ),
                    'str(\\s|$)', 'straße'
                ),
                    ' ', ''
                ),
                    'hbf', 'hauptbahnhof'
                ),
                    'bf', 'bahnhof'
                );
    """)

    # Normalize staging_movements
    cur.execute("""
        UPDATE staging_movements
        SET station_name_normalized =
                REPLACE(
                REPLACE(
                REPLACE(
                REPLACE(
                REPLACE(
                regexp_replace(
                REPLACE(
                REPLACE(
                REPLACE(
                    LOWER(station_xml_name),
                    'berlin', ''
                ),
                    '-', ' '
                ),
                    '.', ' '
                ),
                    'str(\\s|$)', 'straße'
                ),
                    ' ', ''
                ),
                    'hbf', 'hauptbahnhof'
                ),
                    'bf', 'bahnhof'
                ),
                    '(s1)', '(großgörschenstraße)'
                ),
                    '(s2)', ''
                );
    """)

    conn.commit()
    cur.close()
    conn.close()

if __name__ == "__main__":
    main()
