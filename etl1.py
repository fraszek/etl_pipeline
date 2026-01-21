import json
import psycopg2

file_path = 'station_data.json'
with open(file_path, 'r') as file:
    data = json.load(file)

connection = psycopg2.connect(
    dbname='DIA_Assignment',
    user='postgres',
    password='Youmy123',
    host='localhost',
    port='5433'
)

cursor = connection.cursor()

records = []

for station in data['result']:
    name = station['name']
    
    for eva in station['evaNumbers']:
        eva_num = eva['number']
        eva_long = eva['geographicCoordinates']['coordinates'][0]
        eva_lat = eva['geographicCoordinates']['coordinates'][1]
        
        # Only taking the Main EVA number for the station
        if eva.get('isMain'):
            records.append((
                name,
                eva_num,
                eva_long,
                eva_lat
            ))

# Clean the table before reloading
# cursor.execute("DELETE FROM dim_stations;")

# Insert using the new Star Schema names
cursor.executemany("""
    INSERT INTO dim_stations (
        station_name,
        eva_id,
        longitude,
        latitude
    ) VALUES (%s, %s, %s, %s)
""", records)

connection.commit()
cursor.close()
connection.close()

print(f"Successfully loaded {len(records)} stations into dim_stations.")