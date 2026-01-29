from collections import defaultdict, deque
import psycopg2

DB_PARAMS = {
    "host": "localhost",
    "port": 5432,
    "dbname": "postgres",
    "user": "postgres",
    "password": "postgres",
}

def bfs_shortest_path(graph, start, goal):
    queue = deque([(start, [start])])
    visited = set()
    while queue:
        current, path = queue.popleft()
        if current == goal:
            return path
        visited.add(current)
        for neighbor in graph.get(current, []):
            if neighbor not in visited:
                queue.append((neighbor, path + [neighbor]))
    return None

def main():
    conn = psycopg2.connect(**DB_PARAMS)
    cur = conn.cursor()

    graph = defaultdict(set)

    cur.execute("""
        WITH stop_ordered AS (
    SELECT
        f.train_key,
        f.station_key,
        (string_to_array(f.movement_id, '-'))[
            array_length(string_to_array(f.movement_id, '-'), 1)
        ]::INT AS stop_idx,
        LEAD(f.station_key) OVER (
            PARTITION BY f.train_key
            ORDER BY (string_to_array(f.movement_id, '-'))[
                        array_length(string_to_array(f.movement_id, '-'), 1)
                   ]::INT
        ) AS next_station,
        LEAD(
            (string_to_array(f.movement_id, '-'))[
                array_length(string_to_array(f.movement_id, '-'), 1)
            ]::INT
        ) OVER (
            PARTITION BY f.train_key
            ORDER BY (string_to_array(f.movement_id, '-'))[
                        array_length(string_to_array(f.movement_id, '-'), 1)
                   ]::INT
        ) AS next_stop_idx
    FROM fact_train_movements f
    WHERE f.arrival_status != 'a'
      AND f.departure_status != 'a'
      AND (string_to_array(f.movement_id, '-'))[
            array_length(string_to_array(f.movement_id, '-'), 1)
          ]::INT < 100
)
SELECT DISTINCT train_key, station_key, next_station
FROM stop_ordered
WHERE next_station IS NOT NULL
  AND next_stop_idx = stop_idx + 1
ORDER BY train_key, stop_idx;


    """)

    for train, u, v in cur.fetchall():
        if u != v:
            graph[u].add(v)

    print(graph)

    cur.execute("SELECT eva_id FROM dim_stations WHERE station_name = %s", (START,))
    start_row = cur.fetchone()
    if start_row is None:
        raise ValueError(f"Start station '{START}' not found")
    start_id = start_row[0]

    cur.execute("SELECT eva_id FROM dim_stations WHERE station_name = %s", (GOAL,))
    goal_row = cur.fetchone()
    if goal_row is None:
        raise ValueError(f"Goal station '{GOAL}' not found")
    goal_id = goal_row[0]


    path = bfs_shortest_path(graph, start_id, goal_id)

    cur.execute("SELECT eva_id, station_name FROM dim_stations")
    id_to_name = {row[0]: row[1] for row in cur.fetchall()}

    print(len(graph))
    for i in graph.keys():
        
        print("station: ", id_to_name[i])
        print("connections: ", [id_to_name[j] for j in graph[i]])

    print("Shortest path (station names):", [id_to_name[i] for i in path])
    print("Shortest path length: ", len(path)-1)

if __name__ == "__main__":
    START = 'Ahrensfelde'
    GOAL = 'Warschauer Straße'
    main()
