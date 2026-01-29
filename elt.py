from load_stations import load_stations
from load_movements import load_movements
from normalize import normalize_station_names
from match import match_stations


DB_PARAMS = {
    "host": "localhost",
    "port": 5432,
    "dbname": "railway_db",
    "user": "postgres",
    "password": "postgres",
}

# Prerequisite:
# Run schema.sql once to create the database schema and tables before executing this ETL pipeline.

# Expected directory structure:
# - station_data.json
# - timetables/
# - timetable_changes/
# All located at the same level as this pipeline script.

def main():
    print("Starting ETL pipeline")

    print("Step 1: Loading stations")
    load_stations(DB_PARAMS)

    print("Step 2: Loading timetables")
    load_movements(DB_PARAMS)

    print("Step 3: Normalizing station names")
    normalize_station_names(DB_PARAMS)

    print("Step 4: Matching stations to timetables")
    match_stations(DB_PARAMS)

    print("ETL pipeline finished successfully")


if __name__ == "__main__":
    main()