from datetime import datetime, timedelta, date
import hashlib
import json
import logging
import zipfile
import pandas as pd
from pathlib import Path
from airflow.decorators import dag, task
import requests
import psycopg2
from psycopg2.extras import execute_batch

@dag(
    schedule=timedelta(days=1),
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["example"],
)
def static_gtfs_job():
    STATIC_GTFS_URL = "https://gtfs.tpbi.ro/regional/STB.zip"
    EXTRACTED_FILE_BASE_PATH = Path("/opt/airflow/data/gtfs/")

    @task()
    def download():
        logging.info("Downloading Static GTFS zip file")

        run_date = date.today().isoformat()
        run_path = EXTRACTED_FILE_BASE_PATH / run_date
        run_path.mkdir(parents=True, exist_ok=True)

        zip_path = run_path / "gtfs.zip"

        try:
            response = requests.get(STATIC_GTFS_URL, timeout=60)
            response.raise_for_status()
        except requests.RequestException as exc:
            logging.error("Failed to download GTFS file", exc_info=exc)
            raise

        zip_path.write_bytes(response.content)

        logging.info("GTFS file downloaded successfully to %s", zip_path)

        return str(zip_path)

    @task()
    def extract(file_path: str):
        logging.info("Extracting files from zip archive at path %s", file_path)
        run_dir = Path(file_path)
        extract_dir = run_dir.parent / "extracted"
        execution_date = file_path.split("/")[4]

        extract_dir.mkdir(parents=True, exist_ok=True)

        if not run_dir.exists():
            raise FileNotFoundError(f"GTFS zip not found: {file_path}")

        with zipfile.ZipFile(run_dir, "r") as zip_ref:
            zip_ref.extractall(extract_dir)

        def load_table(filename: str) -> pd.DataFrame:
            path = extract_dir / filename
            if not path.exists():
                raise FileNotFoundError(f"Missing GTFS file: {filename}")

            return pd.read_csv(
                path,
                dtype=str,
                na_values=["", " "],
                keep_default_na=False
            )

        routes = load_table("routes.txt")
        stops = load_table("stops.txt")
        trips = load_table("trips.txt")
        stop_times = load_table("stop_times.txt")
        calendar = load_table("calendar.txt")

        sha256 = hashlib.sha256()
        with open(run_dir, "rb") as f:
            for chunk in iter(lambda: f.read(8192), b""):
                sha256.update(chunk)

        checksum = sha256.hexdigest()

        gtfs_json = {
            "metadata": {
                "execution_date": execution_date,
                "checksum": checksum,
                "source": "GTFS static feed"
            },
            "routes": routes.to_dict(orient="records"),
            "stops": stops.to_dict(orient="records"),
            "trips": trips.to_dict(orient="records"),
            "stop_times": stop_times.to_dict(orient="records"),
            "calendar": calendar.to_dict(orient="records")
        }

        return gtfs_json

    @task()
    def process(gtfs: dict):
        logging.info("Processing and validating GTFS data")

        routes = pd.DataFrame(gtfs["routes"])
        stops = pd.DataFrame(gtfs["stops"])
        trips = pd.DataFrame(gtfs["trips"])
        stop_times = pd.DataFrame(gtfs["stop_times"])
        calendar = pd.DataFrame(gtfs["calendar"])

        def normalize_ids(df, cols):
            for c in cols:
                if c in df.columns:
                    df[c] = df[c].astype(str).str.strip()
            return df

        routes = normalize_ids(routes, ["route_id"])
        stops = normalize_ids(stops, ["stop_id"])
        trips = normalize_ids(trips, ["trip_id", "route_id", "service_id"])
        stop_times = normalize_ids(stop_times, ["trip_id", "stop_id"])
        calendar = normalize_ids(calendar, ["service_id"])

        trips = trips[trips["route_id"].isin(routes["route_id"])]
        stop_times = stop_times[stop_times["trip_id"].isin(trips["trip_id"])]
        stop_times = stop_times[stop_times["stop_id"].isin(stops["stop_id"])]
        trips = trips[trips["service_id"].isin(calendar["service_id"])]

        stops["stop_lat"] = stops["stop_lat"].astype(float)
        stops["stop_lon"] = stops["stop_lon"].astype(float)

        stop_times["stop_sequence"] = stop_times["stop_sequence"].astype(int)

        stops["geom"] = stops.apply(
            lambda r: {
                "type": "Point",
                "coordinates": [r.stop_lon, r.stop_lat]
            },
            axis=1
        )
        routes = routes.drop_duplicates(subset=["route_id"])
        stops = stops.drop_duplicates(subset=["stop_id"])
        trips = trips.drop_duplicates(subset=["trip_id"])
        stop_times = stop_times.drop_duplicates(
            subset=["trip_id", "stop_id", "stop_sequence"]
        )

        validated_gtfs = {
            "metadata": {
                **gtfs["metadata"],
                "validated_at": datetime.utcnow().isoformat()
            },
            "routes": routes.to_dict(orient="records"),
            "stops": stops.to_dict(orient="records"),
            "trips": trips.to_dict(orient="records"),
            "stop_times": stop_times.to_dict(orient="records"),
            "calendar": calendar.to_dict(orient="records")
        }

        return {
            "gtfs": validated_gtfs,
            "routes": len(routes),
            "stops": len(stops),
            "trips": len(trips)
        }

    @task()
    def load(data: dict):
        logging.info("Loading GTFS static data into Postgres")

        gtfs = data["gtfs"]
        metadata = gtfs["metadata"]

        conn = psycopg2.connect(
            host="postgis",
            port=5432,
            dbname="gtfs",
            user="gtfs_user",
            password="gtfs_password"
        )
        conn.autocommit = False

        def gtfs_time_to_seconds(t: str | None) -> int | None:
            if not t or pd.isna(t):
                return None
            h, m, s = map(int, t.split(":"))
            return h * 3600 + m * 60 + s

        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO gtfs_versions (checksum, source_url)
                    VALUES (%s, %s)
                    RETURNING version_id
                    """,
                    (
                        metadata["checksum"],
                        "https://gtfs.tpbi.ro/regional/STB.zip"
                    )
                )
                version_id = cur.fetchone()[0]
                logging.info("Created GTFS version_id=%s", version_id)

                execute_batch(
                    cur,
                    """
                    INSERT INTO routes (
                        route_id,
                        route_short_name,
                        route_long_name,
                        route_type,
                        version_id
                    )
                    VALUES (%s, %s, %s, %s, %s)
                    """,
                    [
                        (
                            r.get("route_id"),
                            r.get("route_short_name"),
                            r.get("route_long_name"),
                            r.get("route_type"),
                            version_id
                        )
                        for r in gtfs["routes"]
                    ],
                    page_size=1000
                )

                execute_batch(
                    cur,
                    """
                    INSERT INTO stops (
                        stop_id,
                        stop_name,
                        stop_lat,
                        stop_lon,
                        geom,
                        version_id
                    )
                    VALUES (
                        %s, %s, %s, %s,
                        ST_SetSRID(ST_MakePoint(%s, %s), 4326),
                        %s
                    )
                    """,
                    [
                        (
                            s.get("stop_id"),
                            s.get("stop_name"),
                            s.get("stop_lat"),
                            s.get("stop_lon"),
                            s.get("stop_lon"),
                            s.get("stop_lat"),
                            version_id
                        )
                        for s in gtfs["stops"]
                    ],
                    page_size=1000
                )

                execute_batch(
                    cur,
                    """
                    INSERT INTO calendar (
                        service_id,
                        monday, tuesday, wednesday, thursday,
                        friday, saturday, sunday,
                        start_date, end_date,
                        version_id
                    )
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    """,
                    [
                        (
                            c.get("service_id"),
                            c.get("monday") == "1",
                            c.get("tuesday") == "1",
                            c.get("wednesday") == "1",
                            c.get("thursday") == "1",
                            c.get("friday") == "1",
                            c.get("saturday") == "1",
                            c.get("sunday") == "1",
                            c.get("start_date"),
                            c.get("end_date"),
                            version_id
                        )
                        for c in gtfs["calendar"]
                    ],
                    page_size=1000
                )

                execute_batch(
                    cur,
                    """
                    INSERT INTO trips (
                        trip_id,
                        route_id,
                        service_id,
                        direction_id,
                        version_id
                    )
                    VALUES (%s,%s,%s,%s,%s)
                    """,
                    [
                        (
                            t.get("trip_id"),
                            t.get("route_id"),
                            t.get("service_id"),
                            t.get("direction_id"),
                            version_id
                        )
                        for t in gtfs["trips"]
                    ],
                    page_size=1000
                )

                execute_batch(
                    cur,
                    """
                    INSERT INTO stop_times (
                        trip_id,
                        stop_id,
                        stop_sequence,
                        arrival_time,
                        departure_time,
                        version_id
                    )
                    VALUES (%s,%s,%s,%s,%s,%s)
                    """,
                    [
                        (
                            st.get("trip_id"),
                            st.get("stop_id"),
                            st.get("stop_sequence"),
                            gtfs_time_to_seconds(st.get("arrival_time")),
                            gtfs_time_to_seconds(st.get("departure_time")),
                            version_id
                        )
                        for st in gtfs["stop_times"]
                    ],
                    page_size=2000
                )
            conn.commit()
            logging.info("GTFS static data loaded successfully")
            return version_id

        except Exception:
            conn.rollback()
            logging.exception("Failed to load GTFS static data")
            raise

        finally:
            conn.close()

    @task()
    def init_stop_base_demand(version_id: int):
        conn = psycopg2.connect(
            host="postgis",
            port=5432,
            dbname="gtfs",
            user="gtfs_user",
            password="gtfs_password"
        )

        with conn, conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO stop_base_demand (stop_id, version_id, base_weight)
                SELECT
                    st.stop_id,
                    %s,
                    (5 + floor(random() * 46))::integer
                FROM stop_times st
                WHERE st.version_id = %s
                GROUP BY st.stop_id
                ON CONFLICT DO NOTHING;
                """,
                (version_id, version_id)
            )

        conn.close()
    
    zip_path = download()
    order_data = extract(zip_path)
    order_summary = process(order_data)
    version_id = load(order_summary)
    init_stop_base_demand(version_id)
static_gtfs_job()