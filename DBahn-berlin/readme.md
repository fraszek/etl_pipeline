# DBahn-Berlin: Public Transport Data Analysis

Solution code for the **DIA WiSe2025 Exercise: Berlin Public Transport Data Analysis**.

The exercise is to ingest and analyze real-world data from Berlin's public transport system, collected
through the [Deutsche Bahn API Marketplace](https://developers.deutschebahn.com/db-api-marketplace/apis/).
The dataset covers **133 Berlin stations** from **Sep 02, 2025 to Oct 15, 2025**. It includes planned
train movements and disruptions (delays, cancellations).

---

## Repository structure

```
DBahn-berlin/
├─ readme.md
├─ requirements.txt
├─ sql/
│  └─ station_lookup_queries.sql     # Task 2.1 + 2.2: station lookup / nearest station (PostgreSQL)
└─ spark/
   ├─ etl_movements_to_parquet.py    # Task 3.1: Spark ETL, XML -> Parquet
   ├─ flatten_movements.xslt         # XSLT used by the ETL to flatten each XML file
   ├─ query_avg_daily_delay.py       # Task 3.2: average daily delay for one station
   └─ query_peak_hour_departures.py  # Task 3.3: average peak-hour departures per station
```

---

## Exercise overview

| Part | Task | Description | Where |
|---|---|---|---|
| **1. Schema & ingestion** | 1.1 | Star schema: one fact table for train movements (planned arrivals/departures, delays, cancellations) plus dimension tables (stations, trains, time) | not in this repo |
| | 1.2 | ETL pipeline that parses the `.json` / `.xml` files and loads them into PostgreSQL | not in this repo |
| **2. SQL analysis** | 2.1 | Given a station name, return its coordinates and identifier | `sql/station_lookup_queries.sql` |
| | 2.2 | Given latitude/longitude, return the closest station | `sql/station_lookup_queries.sql` |
| | 2.3 | Given a snapshot (date + hour), return the total number of cancelled trains across all stations | not in this repo |
| | 2.4 | Given a station name, return its average train delay | not in this repo |
| **3. Spark** | 3.1 | Spark ETL job that parses timetable and change files and stores them as Parquet | `spark/etl_movements_to_parquet.py` |
| | 3.2 | For a given station, compute the average daily delay over the collection period | `spark/query_avg_daily_delay.py` |
| | 3.3 | Average number of departures per station during peak hours (07:00–09:00, 17:00–19:00) | `spark/query_peak_hour_departures.py` |
| **4. Graph analytics** | 4.1 | Shortest path between two stations by number of hops | not in this repo |
| | 4.2 | Earliest-arrival routing using timetable information | not in this repo |

---

## Setup

**Requirements**
- Python 3.9+
- Java 17 (for Spark). On Windows, also a Hadoop `winutils.exe` (`HADOOP_HOME`).
- PostgreSQL (for the SQL queries)

```bash
pip install -r requirements.txt
```

**Configure paths.** The scripts use hard-coded local paths. Before running, edit the constants at the
top of each script:

| Script | Constants to edit |
|---|---|
| `spark/etl_movements_to_parquet.py` | `JAVA_HOME`, `HADOOP_HOME`, `SOURCES`, `OUTPUT_MOVEMENTS`, `XSLT_FILE` |
| `spark/query_avg_daily_delay.py` | `JAVA_HOME`, `OUTPUT_PARQUET`, `STATION_NAME` |
| `spark/query_peak_hour_departures.py` | `JAVA_HOME`, `OUTPUT_PARQUET` |

---

## How to run

### Part 2: SQL queries (Tasks 2.1, 2.2)

The queries expect a `dim_stations(eva_id, name, latitude, longitude)` table in PostgreSQL. They are
prepared statements: run the `PREPARE` block once per session, then `EXECUTE` it with your own parameters.

```sql
-- Task 2.1: coordinates and EVA id of a station
EXECUTE simple_select('Ahrensfelde');

-- Task 2.2: nearest station to a (latitude, longitude)
EXECUTE closest_station(52.5692, 13.6081);
```

Task 2.2 ranks stations by squared Euclidean distance in degrees. That is accurate enough to find the
nearest station within Berlin.

### Part 3: Spark (Tasks 3.1–3.3)

Run the ETL once, then either query:

```bash
python spark/etl_movements_to_parquet.py      # Task 3.1: builds staging_movements.parquet
python spark/query_avg_daily_delay.py         # Task 3.2
python spark/query_peak_hour_departures.py    # Task 3.3
```

**Task 3.1: ETL pipeline.** For every XML file in the timetable and change folders:
1. `flatten_movements.xslt` turns the nested `<s>/<ar>/<dp>/<tl>` elements into flat `<row>` elements.
2. Python parses the `YYMMDDHHmm` timestamps and takes `trip_date` from the snapshot folder name and
   `trip_id` from the `stop_id`.
3. Rows are buffered and written to Parquet in batches of 100,000.

For timetable files, the actual time defaults to the planned time. Change files add another row that
holds the updated (actual) time. Cancelled stops (`cs="c"`) get `NULL` as their actual time.

**Resulting Parquet schema**

| Column | Type | Meaning |
|---|---|---|
| `stop_id` | string | DB id of the stop event |
| `trip_date` | string | `YYMMDD` of the snapshot folder |
| `trip_id` | string | Trip part of `stop_id` |
| `station_xml_name` | string | Station name from the XML |
| `arrival_planned` / `arrival_actual` | timestamp | Planned / actual arrival |
| `arrival_status` | string | Change status (`c` = cancelled) |
| `departure_planned` / `departure_actual` | timestamp | Planned / actual departure |
| `departure_status` | string | Change status (`c` = cancelled) |

**Task 3.2: average daily delay.** Filters one station and computes
`delay = departure_actual - departure_planned` in minutes. It then groups by the planned departure
date and returns the average delay and the number of departures per day.

**Task 3.3: peak-hour departures.** De-duplicates on `(stop_id, trip_date)` and drops cancelled
departures. It keeps planned departure hours 7, 8, 17 and 18, counts departures per station per day,
and averages those counts over all days for each station.

---

## Data source

The raw data (not included in this repo) is delivered as weekly `.tar.gz` archives:

- **Stations**: a `.json` file with metadata (name, EVA number, coordinates) for the 133 stations.
- **Timetables** (`timetables/`): planned train movements (arrival/departure times, platforms, lines,
  routes), collected **once per hour** at HH:01. Folder name `YYMMDDHH00`, e.g. `2509051100` =
  Sep 05, 2025, 11:00.
- **Timetable changes** (`timetable_changes/`): delays, cancellations and messages, collected **every
  15 minutes** (HH:01, HH:16, HH:31, HH:46). Folder name `YYMMDDHHmm`, e.g. `2509051115`.

**Weekly containers:** Each archive covers a fixed 7-day window starting at the earliest snapshot date.
The file name `YYMMDD_YYMMDD.tar.gz` gives the start and end of the week. The end date is exclusive.

### On-disk layout (archives)
```bash
.
├─ timetables/
│  ├─ 250902_250909.tar.gz
│  ├─ 250909_250916.tar.gz
│  └─ …
└─ timetable_changes/
   ├─ 250902_250909.tar.gz
   ├─ 250909_250916.tar.gz
   └─ …
```

### Inside an archive (example)
```bash
250902_250909.tar.gz
├─ 2509021200/   # 2025-09-02 12:00
│  ├─ <station>_timetable.xml
│  └─ …
├─ 2509021300/   # 2025-09-02 13:00
└─ …             # up to, but not including, 2025-09-09 00:00
```

In `timetable_changes/`, the folders are 15 minutes apart (e.g. `2509021215/`, `2509021230/`, `2509021245/`).

> **Note for the Spark ETL:** extract each archive into a folder named after it (e.g.
> `timetables/250902_250909/2509021200/...`). The ETL expects `<source>/<week>/<snapshot>/*.xml`.
