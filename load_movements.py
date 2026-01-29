from functools import lru_cache
import psycopg2
from pathlib import Path
from lxml import etree
from datetime import date, time, datetime
from tqdm import tqdm

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
    d = parse_date(ts)
    t = parse_time(ts)
    if d is None or t is None:
        return None
    return datetime.combine(d, t)

def safe_get(attr):
    return attr.strip() if attr and attr.strip() != "" else None

def load_movements(db_params):
    xslt = etree.XSLT(etree.parse("flatten_movements.xslt"))

    conn = psycopg2.connect(**db_params)
    cur = conn.cursor()
    row_counter = 0

    for source in SOURCES:
        if not source.exists():
            continue

        for range_dir in source.iterdir():
            if not range_dir.is_dir():
                continue

            ts_dirs = sorted(d for d in range_dir.iterdir() if d.is_dir())

            desc = f"Ingesting {range_dir.name} changes" if "changes" in source.name else f"Ingesting {range_dir.name}"
            for ts_dir in tqdm(ts_dirs, desc=desc):
                trip_date = ts_dir.name[:8]

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

                            ar_pt_ts = parse_timestamp(safe_get(row.get("ar_pt")))
                            ar_ct_ts = parse_timestamp(safe_get(row.get("ar_ct"))) or ar_pt_ts
                            dp_pt_ts = parse_timestamp(safe_get(row.get("dp_pt")))
                            dp_ct_ts = parse_timestamp(safe_get(row.get("dp_ct"))) or dp_pt_ts

                            ar_status = safe_get(row.get("ar_cs"))
                            if ar_status == "c":
                                ar_ct_ts = None

                            dp_status = safe_get(row.get("dp_cs"))
                            if dp_status == "c":
                                dp_ct_ts = None

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
                                    stop_id, trip_date, trip_id, station_xml_name,
                                    arrival_planned, arrival_actual, arrival_status,
                                    departure_planned, departure_actual, departure_status
                                )
                                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                                ON CONFLICT (stop_id) DO UPDATE SET
                                    trip_date = EXCLUDED.trip_date,
                                    arrival_actual = EXCLUDED.arrival_actual,
                                    departure_actual = EXCLUDED.departure_actual,
                                    arrival_status = EXCLUDED.arrival_status,
                                    departure_status = EXCLUDED.departure_status;
                            """, (
                                stop_id, trip_date, trip_id, station_xml_name,
                                ar_pt_ts, ar_ct_ts, ar_status,
                                dp_pt_ts, dp_ct_ts, dp_status
                            ))

                            row_counter += 1
                            if row_counter >= ROW_COMMIT_THRESHOLD:
                                conn.commit()
                                row_counter = 0

                    except Exception as e:
                        print(f"Skipping {xml_file} due to error: {e}")
                        conn.rollback()
                        continue

    if row_counter > 0:
        conn.commit()

    cur.close()
    conn.close()
    print("Finished loading timetables")
