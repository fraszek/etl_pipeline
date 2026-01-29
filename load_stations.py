import json
import psycopg2

def load_stations(db_params, file_path="station_data.json"):
    with open(file_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    conn = psycopg2.connect(**db_params)
    cur = conn.cursor()

    records = []

    for station in data["result"]:
        name = station["name"]
        for eva in station["evaNumbers"]:
            if eva.get("isMain"):
                records.append((
                    name,
                    eva["number"],
                    eva["geographicCoordinates"]["coordinates"][0],
                    eva["geographicCoordinates"]["coordinates"][1]
                ))

    cur.executemany("""
        INSERT INTO dim_stations (
            station_name, eva_id, longitude, latitude
        )
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (eva_id) DO NOTHING
    """, records)

    conn.commit()
    cur.close()
    conn.close()

    print(f"Loaded {len(records)} stations")
