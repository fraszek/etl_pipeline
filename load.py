from functools import lru_cache
import psycopg2
from pathlib import Path
from lxml import etree
from xml.etree import ElementTree as ET
from datetime import date, time, datetime
from tqdm import tqdm

# -----------------------------
# DB
# -----------------------------

DB_PARAMS = {
    "host": "localhost",
    "port": 5433,
    "dbname": "DIA_Assignment",
    "user": "postgres",
    "password": "Youmy123",
}

SOURCES = [Path("timetables"), Path("timetable_changes")]
ROW_COMMIT_THRESHOLD = 50_000

# -----------------------------
# Cached date/time parsing
# -----------------------------

@lru_cache(maxsize=300_000)
def parse_date(ts):
    if not ts or len(ts) < 6:
        return None
    try:
        return date(2000 + int(ts[0:2]), int(ts[2:4]), int(ts[4:6]))
    except:
        return None


@lru_cache(maxsize=300_000)
def parse_time(ts):
    if not ts or len(ts) < 10:
        return None
    try:
        return time(int(ts[6:8]), int(ts[8:10]))
    except:
        return None


def parse_timestamp(ts):
    """Return datetime or None if invalid"""
    d = parse_date(ts)
    t = parse_time(ts)
    if d is None or t is None:
        return None
    return datetime.combine(d, t)


def is_peak(pt):
    """Return True if hour in peak"""
    if not pt or len(pt) < 8:
        return False
    h = int(pt[6:8])
    return (7 <= h < 9) or (17 <= h < 19)


# -----------------------------
# Safe getter
# -----------------------------

def safe_get(attr):
    """Convert empty strings to None"""
    if attr is None or attr.strip() == "":
        return None
    return attr.strip()


# -----------------------------
# Load XSLT once
# -----------------------------

xslt = etree.XSLT(etree.parse("flatten_movements.xslt"))

# -----------------------------
# Connect to DB
# -----------------------------

conn = psycopg2.connect(**DB_PARAMS)
cur = conn.cursor()
row_counter = 0

# -----------------------------
# Main ingestion loop
# -----------------------------

for source in SOURCES:
    if not source.exists():
        continue

    for range_dir in source.iterdir():
        if not range_dir.is_dir():
            continue

        ts_dirs = sorted(d for d in range_dir.iterdir() if d.is_dir())

        for ts_dir in tqdm(ts_dirs, desc=f"Ingesting {range_dir.name}"):

            observation_date = parse_date(ts_dir.name)

            for xml_file in ts_dir.glob("*.xml"):
                try:
                    tree = etree.parse(xml_file)
                    result = xslt(tree)
                    root = result.getroot()
                    station_xml_name = root.attrib.get("station")

                    for row in root.iter("row"):
                        stop_id = safe_get(row.get("stop_id"))
                        if not stop_id:
                            continue

                        parts = stop_id.split("-")
                        trip_id = parts[0] if not stop_id.startswith("-") else f"-{parts[1]}"

                        # parse timestamps safely
                        ar_pt_ts = parse_timestamp(safe_get(row.get("ar_pt")))
                        ar_ct_ts = parse_timestamp(safe_get(row.get("ar_ct")) or safe_get(row.get("ar_pt")))
                        dp_pt_ts = parse_timestamp(safe_get(row.get("dp_pt")))
                        dp_ct_ts = parse_timestamp(safe_get(row.get("dp_ct")) or safe_get(row.get("dp_pt")))

                        cur.execute("""
                            INSERT INTO dim_trains (
                                trip_id, train_category, train_number, operator
                            )
                            VALUES (%s,%s,%s,%s)
                            ON CONFLICT (trip_id) DO NOTHING;
                        """, (
                            trip_id, safe_get(row.get("tl_c")), safe_get(row.get("tl_n")), safe_get(row.get("tl_o"))
                        ))

                        cur.execute("""
                            INSERT INTO staging_movements (
                                stop_id, observation_date, trip_id, station_xml_name,
                                arrival_planned, arrival_actual, arrival_status,
                                departure_planned, departure_actual, departure_status
                            )
                            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                            ON CONFLICT (stop_id) DO UPDATE SET
                                observation_date = EXCLUDED.observation_date,
                                arrival_actual = EXCLUDED.arrival_actual,
                                departure_actual = EXCLUDED.departure_actual;
                        """, (
                            stop_id, observation_date, trip_id, station_xml_name,
                            ar_pt_ts, ar_ct_ts, safe_get(row.get("ar_cs")),
                            dp_pt_ts, dp_ct_ts, safe_get(row.get("dp_cs"))
                        ))

                        

                        row_counter += 1
                        if row_counter >= ROW_COMMIT_THRESHOLD:
                            conn.commit()
                            row_counter = 0

                except Exception as e:
                    # rollback current transaction to continue
                    print(f"Skipping {xml_file} due to error: {e}")
                    conn.rollback()
                    continue

# Final commit
if row_counter > 0:
    conn.commit()

cur.close()
conn.close()
print("Ingestion finished!")
