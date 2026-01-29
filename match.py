import psycopg2

def match_stations(db_params):
    conn = psycopg2.connect(**db_params)
    cur = conn.cursor()

    cur.execute("CREATE EXTENSION IF NOT EXISTS pg_trgm;")

    cur.execute("""
        INSERT INTO fact_train_movements (
            movement_id,
            observation_date,
            station_key,
            train_key,
            planned_arrival,
            actual_arrival,
            arrival_status,
            planned_departure,
            actual_departure,
            departure_status
        )
        WITH matches AS (
            SELECT 
                s.stop_id, 
                d.eva_id AS station_id,
                similarity(s.station_name_normalized, d.station_name_normalized) AS sim
            FROM staging_movements s
            JOIN dim_stations d
              ON LEFT(s.station_name_normalized, 4) = LEFT(d.station_name_normalized, 4)
             AND similarity(s.station_name_normalized, d.station_name_normalized) > 0.4
        ),
        best_matches AS (
            SELECT DISTINCT ON (stop_id)
                stop_id,
                station_id
            FROM matches
            ORDER BY stop_id, sim DESC
        )
        SELECT 
            s.stop_id,
            s.trip_date,
            m.station_id,
            s.trip_id,
            s.arrival_planned,
            s.arrival_actual,
            s.arrival_status,
            s.departure_planned,
            s.departure_actual,
            s.departure_status
        FROM staging_movements s
        JOIN best_matches m
          ON s.stop_id = m.stop_id;
    """)

    conn.commit()
    cur.close()
    conn.close()
    print("Finished matching stations")
