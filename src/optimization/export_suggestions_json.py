#!/usr/bin/env python3
import os
import json
import math
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd
from sqlalchemy import create_engine, text


# -----------------------------
# DB connection
# -----------------------------
def build_engine():
    running_in_docker = os.getenv("RUNNING_IN_DOCKER", "0") == "1"

    host = os.getenv("DB_HOST", "postgis" if running_in_docker else "localhost")
    port = os.getenv("DB_PORT", "5432")
    db = os.getenv("DB_NAME", "gtfs")
    user = os.getenv("DB_USER", "gtfs_user")
    pwd = os.getenv("DB_PASS", "gtfs_password")

    url = f"postgresql+psycopg2://{user}:{pwd}@{host}:{port}/{db}"
    return create_engine(url, pool_pre_ping=True)


# -----------------------------
# JSON safety helpers
# -----------------------------
def is_bad_number(x) -> bool:
    return isinstance(x, float) and (math.isnan(x) or math.isinf(x))


def clean_json(obj: Any) -> Any:
    """
    Recursively convert pandas NaT / NaN / inf to None so JSON is valid.
    """
    if obj is pd.NaT:
        return None
    if is_bad_number(obj):
        return None
    if isinstance(obj, (pd.Timestamp,)):
        return obj.isoformat()
    if isinstance(obj, dict):
        return {k: clean_json(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [clean_json(v) for v in obj]
    return obj


def ensure_parent_dir(path: Path):
    path.parent.mkdir(parents=True, exist_ok=True)


def write_json(path: Path, payload: Any, pretty: bool = True):
    ensure_parent_dir(path)
    with path.open("w", encoding="utf-8") as f:
        json.dump(
            clean_json(payload),
            f,
            ensure_ascii=False,
            indent=2 if pretty else None,
            allow_nan=False,
        )


# -----------------------------
# SQL queries (UPDATED)
# -----------------------------
SQL_LATEST_ROWS = """
WITH latest AS (
  SELECT MAX(hour_ts) AS hour_ts
  FROM public.kpi_hourly
),
payload AS (
  SELECT
    k.route_id::text AS route_id,
    k.hour_ts,
    k.predicted_passengers,
    k.observed_passengers,
    k.active_vehicles,
    k.avg_headway_min,
    k.is_peak_hour,

    COALESCE(s.action, 'needs_data') AS action,
    s.current_headway_min,
    s.recommended_headway_min,
    COALESCE(s.priority_score, 0.0) AS priority_score,
    COALESCE(
      s.reasons,
      jsonb_build_object('reason', 'no suggestion for this route-hour')
    ) AS reasons
  FROM public.kpi_hourly k
  JOIN latest l ON k.hour_ts = l.hour_ts
  LEFT JOIN public.route_optimization_suggestions s
    ON s.route_id = k.route_id::text
   AND s.hour_ts = k.hour_ts
)
SELECT *
FROM payload
ORDER BY hour_ts, route_id;
"""

SQL_PEAK_HOURS_LATEST = """
WITH latest AS (
  SELECT MAX(hour_ts) AS max_ts
  FROM public.kpi_hourly
),
win AS (
  SELECT *
  FROM public.kpi_hourly
  WHERE hour_ts >= (SELECT max_ts FROM latest) - interval '24 hours'
)
SELECT
  EXTRACT(HOUR FROM hour_ts)::int AS hour_of_day,
  COUNT(*)::int AS n_routes,
  AVG(predicted_passengers)::float AS avg_predicted_passengers,
  AVG(observed_passengers)::float AS avg_observed_passengers,
  AVG(CASE WHEN active_vehicles > 0 THEN active_vehicles ELSE NULL END)::float AS avg_active_vehicles,
  AVG(CASE WHEN avg_headway_min IS NOT NULL THEN avg_headway_min ELSE NULL END)::float AS avg_headway_min,
  SUM(CASE WHEN is_peak_hour = 1 THEN 1 ELSE 0 END)::int AS peak_routes
FROM win
GROUP BY 1
ORDER BY 1;
"""


# -----------------------------
# Frontend-friendly transforms
# -----------------------------
def compute_status(active_vehicles: int, avg_headway_min: Optional[float]) -> str:
    has_veh = active_vehicles is not None and int(active_vehicles) > 0
    has_hw = avg_headway_min is not None and not is_bad_number(float(avg_headway_min))
    if has_veh and has_hw:
        return "ok"
    if (not has_veh) and (not has_hw):
        return "missing_both"
    if not has_veh:
        return "missing_vehicles"
    return "missing_headway"


def compute_message(action: str, status: str) -> str:
    if status != "ok" and action == "needs_data":
        return "Needs data (no vehicles/headway for this route-hour)"
    if action == "increase_frequency":
        return "Increase frequency (over capacity target)"
    if action == "decrease_frequency":
        return "Decrease frequency (under-utilized, off-peak)"
    return "Keep current frequency"


def get_confidence(reasons: Any) -> float:
    if isinstance(reasons, dict) and "confidence" in reasons:
        try:
            return float(reasons["confidence"])
        except Exception:
            return 0.0
    return 0.0


def safe_float(x) -> Optional[float]:
    if x is None:
        return None
    try:
        v = float(x)
        if is_bad_number(v):
            return None
        return v
    except Exception:
        return None


def safe_int(x, default=0) -> int:
    if x is None:
        return default
    try:
        return int(x)
    except Exception:
        return default


# -----------------------------
# Exporters
# -----------------------------
def build_route_rows(df: pd.DataFrame) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []

    for _, r in df.iterrows():
        route_id = str(r["route_id"])
        hour_ts = r["hour_ts"].isoformat() if hasattr(r["hour_ts"], "isoformat") else str(r["hour_ts"])

        active_vehicles = safe_int(r["active_vehicles"], 0)
        avg_headway_min = safe_float(r["avg_headway_min"])
        status = compute_status(active_vehicles, avg_headway_min)

        action = str(r["action"])
        reasons = r["reasons"] if r["reasons"] is not None else {"reason": "no data"}

        current_hw = safe_float(r["current_headway_min"])
        rec_hw = safe_float(r["recommended_headway_min"])

        delta_hw = None
        delta_hw_pct = None
        if current_hw is not None and rec_hw is not None and current_hw > 0:
            delta_hw = rec_hw - current_hw
            delta_hw_pct = (rec_hw / current_hw - 1.0) * 100.0

        payload = {
            "route_id": route_id,
            "hour_ts": hour_ts,
            "kpi": {
                "predicted_passengers": safe_float(r["predicted_passengers"]),
                "observed_passengers": safe_float(r["observed_passengers"]),
                "active_vehicles": active_vehicles,
                "avg_headway_min": avg_headway_min,
                "is_peak_hour": safe_int(r["is_peak_hour"], 0),
            },
            "suggestion": {
                "action": action,
                "status": status,
                "message": compute_message(action, status),
                "current_headway_min": current_hw,
                "recommended_headway_min": rec_hw,
                "delta_headway_min": delta_hw,
                "delta_headway_pct": delta_hw_pct,
                "priority_score": safe_float(r["priority_score"]) or 0.0,
                "confidence": get_confidence(reasons),
                "reasons": reasons,
            },
        }
        rows.append(payload)

    return rows


def export_all(pretty: bool = True):
    engine = build_engine()

    df_latest = pd.read_sql(text(SQL_LATEST_ROWS), engine)
    route_rows = build_route_rows(df_latest)

    out_full = Path(os.getenv("EXPORT_FULL_PATH", "exports/route_suggestions_latest.json"))
    write_json(out_full, route_rows, pretty=pretty)

    needs_data = [x for x in route_rows if x["suggestion"]["action"] == "needs_data"]
    out_needs = Path(os.getenv("EXPORT_NEEDS_DATA_PATH", "exports/needs_data_latest.json"))
    write_json(out_needs, needs_data, pretty=pretty)

    real_sugg = [x for x in route_rows if x["suggestion"]["action"] != "needs_data"]
    real_sugg_sorted = sorted(real_sugg, key=lambda x: x["suggestion"]["priority_score"], reverse=True)
    top_n = int(os.getenv("EXPORT_TOP_N", "20"))
    out_top = Path(os.getenv("EXPORT_TOP_PATH", "exports/top_priority_latest.json"))
    write_json(out_top, real_sugg_sorted[:top_n], pretty=pretty)

    action_counts: Dict[str, int] = {}
    for x in route_rows:
        a = x["suggestion"]["action"]
        action_counts[a] = action_counts.get(a, 0) + 1

    summary = {
        "hour_ts": route_rows[0]["hour_ts"] if route_rows else None,
        "total_routes": len(route_rows),
        "needs_data": len(needs_data),
        "suggestions": len(route_rows) - len(needs_data),
        "actions": action_counts,
        "top_priority": [
            {
                "route_id": x["route_id"],
                "priority_score": x["suggestion"]["priority_score"],
                "action": x["suggestion"]["action"],
                "message": x["suggestion"]["message"],
            }
            for x in real_sugg_sorted[:10]
        ],
    }
    out_summary = Path(os.getenv("EXPORT_SUMMARY_PATH", "exports/summary_latest.json"))
    write_json(out_summary, summary, pretty=pretty)

    try:
        df_peak = pd.read_sql(text(SQL_PEAK_HOURS_LATEST), engine)
        peak_payload = df_peak.to_dict(orient="records")
    except Exception as e:
        peak_payload = {"error": str(e)}
    out_peak = Path(os.getenv("EXPORT_PEAK_PATH", "exports/peak_hours_latest.json"))
    write_json(out_peak, peak_payload, pretty=pretty)

    print("✅ Export done:")
    print(f" - {out_full}")
    print(f" - {out_needs}")
    print(f" - {out_top}")
    print(f" - {out_summary}")
    print(f" - {out_peak}")


if __name__ == "__main__":
    pretty = os.getenv("EXPORT_JSON_PRETTY", "1") != "0"
    export_all(pretty=pretty)
