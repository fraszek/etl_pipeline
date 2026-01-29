from functools import lru_cache
import psycopg2
from pathlib import Path
from lxml import etree
from xml.etree import ElementTree as ET
from datetime import datetime
from tqdm import tqdm
import io
from datetime import date, time, datetime

import time

DB_PARAMS = {
    "host": "localhost",
    "port": 5432,
    "dbname": "DIA_Assignment",
    "user": "postgres",
    "password": "postgres",
}

SOURCES = [Path("C:/Users/frane/Desktop/Minor/DIA/Queries/timetables"), Path("timetable_changes")]
ROW_COMMIT_THRESHOLD = 50_000

# -----------------------------
# Helpers
# -----------------------------

# @lru_cache(maxsize=200_000)
def parse_date(ts):
    if not ts:
        return None
    try:
        return date(
            2000 + int(ts[0:2]),
            int(ts[2:4]),
            int(ts[4:6]),
        )
    except Exception:
        return None

def parse_time(ts):
    if not ts:
        return None
    try:
        return time(
            int(ts[6:8]),
            int(ts[8:10]),
        )
    except Exception:
        return None

def parse_db_dt(ts):
    d = parse_date(ts)
    if d is None:
        return None

    t = parse_time(ts)
    if t is None:
        return None

    return datetime.combine(d, t)

def is_peak_from_pt(pt):
    if not pt:
        return False
    h = int(pt[6:8])
    return (7 <= h < 9) or (17 <= h < 19)

# -----------------------------
# Load XSLT once
# -----------------------------

# xslt = etree.XSLT(etree.parse("flatten_movements.xslt"))

# -----------------------------
# DB
# -----------------------------

conn = psycopg2.connect(**DB_PARAMS)
cur = conn.cursor()

row_counter = 0
# copy_buffer = io.StringIO()

def flush_copy():
    global copy_buffer
    copy_buffer.seek(0)
    cur.copy_from(
        copy_buffer,
        "staging_movements",
        sep="\t",
        columns=(
            "stop_id",
            "observation_ts",
            # "trip_id",
            "station_xml_name",
            "arrival_planned",
            "arrival_actual",
            "arrival_status",
            "departure_planned",
            "departure_actual",
            "departure_status",
            # "is_peak_departure",
        ),
    )
    copy_buffer.close()
    copy_buffer = io.StringIO()

# -----------------------------
# Main loop
# -----------------------------

for source in SOURCES:
    if not source.exists():
        continue

    for range_dir in source.iterdir():
        if not range_dir.is_dir():
            continue

        ts_dirs = sorted(d for d in range_dir.iterdir() if d.is_dir())

        for ts_dir in tqdm(ts_dirs, desc=f"Ingesting {range_dir.name}"):
            # obs_ts = parse_db_dt(ts_dir.name)
            latencies = [0, 0, 0, 0, 0]


            for xml_file in ts_dir.glob("*.xml"):
                with open(xml_file, 'r') as f:
                    start_time = time.time()
                    tree = ET.parse(f).getroot()
                # result = xslt(tree)
                # root = result.getroot()
                # station = root.attrib.get("station")
                # latencies[0] = time.time() - start_time


                # for row in root.iter("row"):
                #     sid = row.get("stop_id")
                #     # parts = sid.split("-")
                #     # tid = parts[0] if not sid.startswith("-") elsef"-{parts[1]}"

                #     ar_pt = row.get("ar_pt")
                #     ar_ct = row.get("ar_ct") or ar_pt

                #     dp_pt = row.get("dp_pt")
                #     dp_ct = row.get("dp_ct") or dp_pt

                #     latencies[1] = time.time() - start_time

                #     # record = [
                #     #     sid,
                #     #     # obs_ts,
                #     #     # tid,
                #     #     station,
                #     #     parse_db_dt(ar_pt),
                #     #     parse_db_dt(ar_ct),
                #     #     row.get("ar_cs"),
                #     #     parse_db_dt(dp_pt),
                #     #     parse_db_dt(dp_ct),
                #     #     row.get("dp_cs"),
                #     #     # is_peak_from_pt(dp_pt),
                #     # ]
                #     latencies[2] = time.time() - start_time



                #     # copy_buffer.write(
                #     #     "\t".join("" if v is None else str(v) forv in record)
                #     #     + "\n"
                #     # )
                #     # latencies[3] = time.time() - start_time



                #     row_counter += 1

                #     # if row_counter >= ROW_COMMIT_THRESHOLD:
                #     #     flush_copy()
                #     #     conn.commit()
                #     #     row_counter = 0

                #     latencies[4] = time.time() - start_time


            # print(latencies)


# Final flush
if row_counter > 0:
    flush_copy()
    conn.commit()

cur.close()
conn.close()
print("Ingestion Finished!")