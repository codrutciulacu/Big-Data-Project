import os
import json
import math
import uuid
import pandas as pd
from sqlalchemy import create_engine, text

# ---------------------------
# Connection (WSL + Docker)
# ---------------------------
DB_HOST = os.getenv("DB_HOST", "postgis" if os.getenv("RUNNING_IN_DOCKER") == "1" else "localhost")
DB_PORT = int(os.getenv("DB_PORT", "5432"))
DB_NAME = os.getenv("DB_NAME", "gtfs")
DB_USER = os.getenv("DB_USER", "gtfs_user")
DB_PASS = os.getenv("DB_PASS", "gtfs_password")

engine = create_engine(
    f"postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}",
    pool_pre_ping=True,
)

# ---------------------------
# Tunables
# ---------------------------
VEH_CAPACITY = 60.0
TARGET_LOAD = 0.75
LOW_LOAD = 0.40

MAX_HEADWAY = 30.0
MIN_HEADWAY = 3.0
DEFAULT_HEADWAY_MIN = 12.0

SAVE_ONLY_CHANGES = True  # recomandat (altfel bagi si "keep")

def clamp(x, lo, hi):
    return max(lo, min(hi, x))

def json_safe(obj):
    if obj is None:
        return None
    try:
        if pd.isna(obj):
            return None
    except Exception:
        pass
    if isinstance(obj, pd.Timestamp):
        return obj.to_pydatetime().isoformat()
    if isinstance(obj, dict):
        return {k: json_safe(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [json_safe(v) for v in obj]
    return obj

# ---------------------------
# Load KPI input
# ---------------------------
df = pd.read_sql(
    """
    SELECT *
    FROM public.kpi_hourly
    ORDER BY hour_ts, route_id
    """,
    engine
)

if df.empty:
    raise SystemExit("kpi_hourly is empty. Ensure demand_predictions has data and KPI views exist.")

df["active_vehicles"] = df["active_vehicles"].fillna(0).astype(int)

def compute_headway(row):
    hw = row.get("avg_headway_min", None)
    if hw is not None and not pd.isna(hw) and 0.5 <= float(hw) <= 60.0:
        return float(hw)

    av = int(row.get("active_vehicles", 0) or 0)
    if av > 0:
        return float(clamp(60.0 / float(av), MIN_HEADWAY, MAX_HEADWAY))

    return float(DEFAULT_HEADWAY_MIN)

df["current_headway_min"] = df.apply(compute_headway, axis=1)
df["trips_per_hour_est"] = df["current_headway_min"].apply(lambda hw: (60.0 / hw) if hw and hw > 0 else None)

df["capacity_est"] = df.apply(
    lambda r: (VEH_CAPACITY * r["trips_per_hour_est"])
    if r["trips_per_hour_est"] is not None and r["trips_per_hour_est"] > 0
    else None,
    axis=1
)

df["load_factor"] = df.apply(
    lambda r: (float(r["predicted_passengers"]) / float(r["capacity_est"]))
    if r["capacity_est"] is not None and r["capacity_est"] > 0 and r["predicted_passengers"] is not None
    else None,
    axis=1
)

df["passengers_per_trip_est"] = df.apply(
    lambda r: (float(r["predicted_passengers"]) / float(r["trips_per_hour_est"]))
    if r["trips_per_hour_est"] is not None and r["trips_per_hour_est"] > 0 and r["predicted_passengers"] is not None
    else None,
    axis=1
)

def decide(row):
    headway = float(row["current_headway_min"])
    tphr = row["trips_per_hour_est"]
    ppt = row["passengers_per_trip_est"]

    peak = int(row.get("is_peak_hour", 0) or 0)
    delay_sec = float(row.get("avg_delay_seconds", 0) or 0)
    delay_norm = clamp(delay_sec / 600.0, 0, 1)  # 10 min => 1.0

    cap_target = VEH_CAPACITY * TARGET_LOAD
    cap_low = VEH_CAPACITY * LOW_LOAD

    lf = row.get("load_factor", None)
    if lf is None or (isinstance(lf, float) and math.isnan(lf)):
        confidence = 0.70
    else:
        confidence = clamp(1.0 - abs(float(lf) - 1.0) * 0.35, 0.55, 1.0)

    hour_ts = row["hour_ts"]
    if isinstance(hour_ts, pd.Timestamp):
        hour_ts = hour_ts.to_pydatetime()

    reasons = {
        "predicted_passengers": float(row["predicted_passengers"]) if row["predicted_passengers"] is not None else None,
        "observed_passengers": float(row["observed_passengers"]) if row.get("observed_passengers") is not None else None,
        "active_vehicles": int(row["active_vehicles"]),
        "avg_headway_min": float(row["avg_headway_min"]) if row.get("avg_headway_min") is not None else None,
        "current_headway_min": float(headway),
        "trips_per_hour_est": float(tphr) if tphr is not None else None,
        "capacity_est_per_hour": float(row["capacity_est"]) if row.get("capacity_est") is not None else None,
        "load_factor_per_hour": float(lf) if lf is not None else None,
        "passengers_per_trip_est": float(ppt) if ppt is not None else None,
        "cap_target_per_trip": float(cap_target),
        "cap_low_per_trip": float(cap_low),
        "avg_delay_seconds": delay_sec,
        "is_peak_hour": peak,
        "confidence": float(confidence),
    }

    action = "keep"
    rec_headway = float(headway)

    if ppt is None or tphr is None:
        reasons["rule"] = "missing trip estimate"
    else:
        pred = float(row["predicted_passengers"]) if row["predicted_passengers"] is not None else 0.0

        if ppt > cap_target:
            action = "increase_frequency"
            tphr_target = (pred / cap_target) if cap_target > 0 else None
            raw_rec = (60.0 / tphr_target) if tphr_target and tphr_target > 0 else headway * 0.85
            raw_rec = float(clamp(raw_rec, MIN_HEADWAY, MAX_HEADWAY))

            rec_headway = float(clamp(
                headway * (1.0 - 0.35 * confidence) + raw_rec * (0.35 * confidence),
                MIN_HEADWAY, MAX_HEADWAY
            ))

            reasons["rule"] = "ppt > cap_target"
            reasons["trips_per_hour_target"] = float(tphr_target) if tphr_target is not None else None
            reasons["raw_recommended_headway_min"] = float(raw_rec)

        elif ppt < cap_low and peak == 0:
            action = "decrease_frequency"
            tphr_target = (pred / cap_target) if cap_target > 0 else None
            raw_rec = (60.0 / tphr_target) if tphr_target and tphr_target > 0 else headway * 1.15
            raw_rec = float(clamp(raw_rec, MIN_HEADWAY, MAX_HEADWAY))

            rec_headway = float(clamp(
                headway * (1.0 - 0.30 * confidence) + raw_rec * (0.30 * confidence),
                MIN_HEADWAY, MAX_HEADWAY
            ))

            reasons["rule"] = "ppt < cap_low and off-peak"
            reasons["trips_per_hour_target"] = float(tphr_target) if tphr_target is not None else None
            reasons["raw_recommended_headway_min"] = float(raw_rec)

        else:
            reasons["rule"] = "within normal range"

    over = 0.0
    if ppt is not None and cap_target > 0:
        over = clamp((float(ppt) - cap_target) / cap_target, 0, 1)

    score = 0.60 * over + 0.20 * peak + 0.20 * delay_norm
    score = float(clamp(score, 0, 1))

    return {
        "route_id": str(row["route_id"]),
        "hour_ts": hour_ts,
        "action": action,
        "current_headway_min": float(headway),
        "recommended_headway_min": float(rec_headway),
        "priority_score": score,
        "reasons": json_safe(reasons),
    }

suggestions = [decide(r) for _, r in df.iterrows()]
suggestions = [s for s in suggestions if s is not None]

if SAVE_ONLY_CHANGES:
    suggestions = [s for s in suggestions if s["action"] != "keep"]

run_id = str(uuid.uuid4())

if suggestions:
    payload = []
    for s in suggestions:
        payload.append({
            "route_id": s["route_id"],
            "hour_ts": s["hour_ts"],
            "action": s["action"],
            "current_headway_min": s["current_headway_min"],
            "recommended_headway_min": s["recommended_headway_min"],
            "priority_score": s["priority_score"],
            "reasons": json.dumps(s["reasons"], ensure_ascii=False, allow_nan=False),
            "model_run_id": run_id,
        })

    with engine.begin() as conn:
        conn.execute(text("""
          INSERT INTO public.route_optimization_suggestions_history
          (route_id, hour_ts, action, current_headway_min, recommended_headway_min, priority_score, reasons, model_run_id)
          VALUES
          (:route_id, :hour_ts, :action, :current_headway_min, :recommended_headway_min, :priority_score,
           CAST(:reasons AS JSONB), CAST(:model_run_id AS uuid))
          ON CONFLICT (route_id, hour_ts, model_run_id) DO NOTHING;
        """), payload)

print(f"Saved {len(suggestions)} suggestions into route_optimization_suggestions_history. run_id={run_id}")
