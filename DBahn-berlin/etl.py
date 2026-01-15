import json
from pickle import INT
import psycopg2

#Change file path as needed
file_path = 'C:/Users/frane/Desktop/Minor/DIA/Assignment/DBahn-berlin/station_data.json'
with open(file_path, 'r') as file:
    data = json.load(file)

#Change password and info as needed
connection = psycopg2.connect(
    dbname='DIA_Assignment',
    user='postgres',
    password='postgres',
    host='localhost',
    port='5432')

cursor = connection.cursor()
total = data['total']

records = []

for station in data['result']:
    name = station['name']
	
    evaNumbers = station['evaNumbers']
    for eva in station['evaNumbers']:
        eva_num = eva['number']
        eva_long= eva['geographicCoordinates']['coordinates'][0]
        eva_lat = eva['geographicCoordinates']['coordinates'][1]
        if(eva['isMain']):
            records.append((name,
                                eva_num,
                                eva_long,
                                eva_lat))


                
print(records[0])            
print(records.__len__())

cursor.execute("DELETE FROM stations;")
cursor.executemany("""INSERT INTO stations (
                        "name",
                        "evanumber",
                        "evanumber_geographiccoordinates_longitude",
                        "evanumber_geographiccoordinates_latitude"
)
        VALUES (%s, %s, %s, %s)
                       """, records)
connection.commit()
