from __future__ import annotations

import os
import json
from pathlib import Path
from typing import Dict, Any, List

import psycopg2
from psycopg2.extras import RealDictCursor

from airflow.decorators import dag, task
from airflow.utils.dates import days_ago


EXPORT_BASE_DIR = Path("/opt/airflow/data_exports")
OUTPUT_FILE = "routes_geo_latest.json"


def get_pg_conn():
    DB_HOST = os.getenv("DB_HOST", "postgis")
    DB_PORT = int(os.getenv("DB_PORT", "5432"))
    DB_NAME = os.getenv("DB_NAME", "gtfs")
    DB_USER = os.getenv("DB_USER", "gtfs_user")
    DB_PASS = os.getenv("DB_PASS", "gtfs_password")

    return psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASS,
        cursor_factory=RealDictCursor,
    )


SQL_ROUTES_GEO = """
SELECT DISTINCT
    r.route_id,
    r.route_short_name,
    r.route_long_name,
    s.stop_id,
    s.stop_name,
    s.stop_lat,
    s.stop_lon
FROM routes r
JOIN trips t ON t.route_id = r.route_id
JOIN stop_times st ON st.trip_id = t.trip_id
JOIN stops s ON s.stop_id = st.stop_id
WHERE s.stop_lat IS NOT NULL 
  AND s.stop_lon IS NOT NULL
ORDER BY r.route_id
LIMIT 1000;
"""


@dag(
    dag_id="export_routes_geo_json",
    start_date=days_ago(1),
    schedule="@daily",  # or "@hourly"
    catchup=False,
    tags=["exports", "geo", "gtfs"],
)
def export_routes_geo_json():
    @task
    def export_routes_geo() -> Dict[str, Any]:
        EXPORT_BASE_DIR.mkdir(parents=True, exist_ok=True)
        output_path = EXPORT_BASE_DIR / OUTPUT_FILE

        conn = get_pg_conn()
        try:
            with conn.cursor() as cur:
                cur.execute(SQL_ROUTES_GEO)
                rows = cur.fetchall()
        finally:
            conn.close()

        # Group stops by route
        routes_map: Dict[str, Dict[str, Any]] = {}

        for row in rows:
            route_id = row["route_id"]

            if route_id not in routes_map:
                routes_map[route_id] = {
                    "route_id": route_id,
                    "route_short_name": row.get("route_short_name"),
                    "route_long_name": row.get("route_long_name"),
                    "stops": [],
                }

            routes_map[route_id]["stops"].append(
                {
                    "stop_id": row["stop_id"],
                    "stop_name": row["stop_name"],
                    "lat": float(row["stop_lat"]),
                    "lon": float(row["stop_lon"]),
                }
            )

        routes_list: List[Dict[str, Any]] = list(routes_map.values())

        # Write JSON
        with output_path.open("w", encoding="utf-8") as f:
            json.dump(routes_list, f, ensure_ascii=False, indent=2)

        return {
            "exported_routes": len(routes_list),
            "output_path": str(output_path),
        }

    export_routes_geo()


export_routes_geo_json()
