import psycopg2
from datetime import datetime

start_time = datetime.strptime("2023-09-09 08:30", "%Y-%m-%d %H:%M")
start_station = "Ahrensfelde"
target_station = "Berlin Zoologischer Garten"

DB_PARAMS = {
    "host": "localhost",
    "port": 5432,
    "dbname": "railway_db",
    "user": "postgres",
    "password": "postgres",
}


def get_cursor():
    conn = psycopg2.connect(**DB_PARAMS)
    conn.autocommit = True
    return conn, conn.cursor()

def earliest_arrival(start_station, target_station, start_time):
    import heapq

    dist = {}
    parent = {}
    visited = set()

    pq = []
    heapq.heappush(pq, (start_time, start_station))
    dist[start_station] = start_time
    parent[start_station] = None

    while pq:
        cur_time, station = heapq.heappop(pq)

        if station in visited:
            continue
        visited.add(station)

        if station == target_station:
            break

        conn, cur = get_cursor()

        cur.execute("""
            WITH ordered AS (
                SELECT
                    f.station_key AS from_station,
                    f.actual_departure AS dep_time,
                    LEAD(f.station_key) OVER w AS to_station,
                    LEAD(f.actual_arrival) OVER w AS arr_time
                FROM fact_train_movements f
                WHERE f.arrival_status != 'c'
                  AND f.departure_status != 'c'
                WINDOW w AS (
                    PARTITION BY f.train_key
                    ORDER BY
                      (string_to_array(f.movement_id, '-'))[
                          array_length(string_to_array(f.movement_id, '-'), 1)
                      ]::INT
                )
            )
            SELECT to_station, arr_time
            FROM ordered
            WHERE from_station = %s
              AND dep_time >= %s
              AND to_station IS NOT NULL;
        """, (station, cur_time))

        for to_station, arr_time in cur.fetchall():
            if to_station not in dist or arr_time < dist[to_station]:
                dist[to_station] = arr_time
                parent[to_station] = station
                heapq.heappush(pq, (arr_time, to_station))

        cur.close()
        conn.close()

    if target_station not in dist:
        return None, None

    path = []
    s = target_station
    while s:
        path.append(s)
        s = parent[s]
    path.reverse()

    return dist[target_station], path


arrival_time, path = earliest_arrival(
    start_station="8011003",        # eva_id
    target_station="8010406",       # eva_id
    start_time=start_time
)

print(arrival_time)
print(path)
