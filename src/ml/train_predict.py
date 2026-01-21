import os
import uuid
import json
import pandas as pd
from sqlalchemy import create_engine, text
from sklearn.metrics import mean_absolute_error
from sklearn.ensemble import RandomForestRegressor

# ---------------------------
# DB connection
# ---------------------------
DB_USER = os.getenv("DB_USER", "gtfs_user")
DB_PASS = os.getenv("DB_PASS", "gtfs_password")
DB_HOST = os.getenv("DB_HOST", "postgis")
DB_PORT = int(os.getenv("DB_PORT", "5432"))
DB_NAME = os.getenv("DB_NAME", "gtfs")

engine = create_engine(
    f"postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}",
    pool_pre_ping=True,
)

# ---------------------------
# Run metadata
# ---------------------------
RUN_ID = str(uuid.uuid4())
MODEL_NAME = "rf_next_hour"
MODEL_VERSION = "v1"

FEATURES = [
    "trip_update_events",
    "avg_delay_seconds",
    "vehicle_events",
    "temperature_c",
    "precipitation_mm",
    "wind_speed_mps",
    "hour",
    "day_of_week",
    "is_peak",
]

# ---------------------------
# Ensure ONLY ml_dataset_hourly exists
# ---------------------------
ENSURE_ML_DATASET_VIEW_SQL = """
DROP VIEW IF EXISTS public.ml_dataset_hourly CASCADE;

CREATE VIEW public.ml_dataset_hourly AS
SELECT
  pd.route_id,
  pd.hour_ts,
  pd.passengers::double precision AS passengers,

  COALESCE(dh.trip_update_events, 0)::bigint AS trip_update_events,
  COALESCE(dh.avg_delay_seconds, 0)::double precision AS avg_delay_seconds,
  COALESCE(vh.vehicle_events, 0)::bigint AS vehicle_events,

  COALESCE(wh.temperature_c, 0)::double precision AS temperature_c,
  COALESCE(wh.precipitation_mm, 0)::double precision AS precipitation_mm,
  COALESCE(wh.wind_speed_mps, 0)::double precision AS wind_speed_mps,

  EXTRACT(HOUR FROM pd.hour_ts)::int AS hour,
  EXTRACT(DOW FROM pd.hour_ts)::int AS day_of_week,
  ((EXTRACT(HOUR FROM pd.hour_ts) BETWEEN 7 AND 9)
   OR (EXTRACT(HOUR FROM pd.hour_ts) BETWEEN 16 AND 18))::int AS is_peak
FROM public.passenger_demand_hourly pd
LEFT JOIN public.delay_hourly dh
  ON pd.route_id = dh.route_id AND pd.hour_ts = dh.hour_ts
LEFT JOIN public.vehicle_hourly vh
  ON pd.route_id = vh.route_id AND pd.hour_ts = vh.hour_ts
LEFT JOIN public.weather_hourly wh
  ON pd.hour_ts = wh.hour_ts;
"""

def ensure_ml_dataset_view():
    with engine.begin() as conn:
        statements = [s.strip() for s in ENSURE_ML_DATASET_VIEW_SQL.split(";") if s.strip()]
        for stmt in statements:
            conn.execute(text(stmt))

def ensure_tables():
    with engine.begin() as conn:
        # run log
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS public.ml_runs (
                run_id UUID PRIMARY KEY,
                model_name TEXT NOT NULL,
                model_version TEXT NOT NULL,
                created_at TIMESTAMP WITHOUT TIME ZONE DEFAULT NOW(),
                rows_used BIGINT,
                split_ratio DOUBLE PRECISION,
                mae DOUBLE PRECISION,
                features JSONB
            );
        """))

        # LATEST predictions
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS public.demand_predictions (
                route_id TEXT NOT NULL,
                hour_ts TIMESTAMP WITHOUT TIME ZONE NOT NULL,
                y_true DOUBLE PRECISION,
                y_pred DOUBLE PRECISION,
                created_at TIMESTAMP WITHOUT TIME ZONE DEFAULT NOW(),
                PRIMARY KEY (route_id, hour_ts)
            );
        """))

        # ✅ MIGRATION for old schema: add cols if missing
        conn.execute(text("""
            ALTER TABLE public.demand_predictions
              ADD COLUMN IF NOT EXISTS run_id UUID,
              ADD COLUMN IF NOT EXISTS model_name TEXT,
              ADD COLUMN IF NOT EXISTS model_version TEXT;
        """))

        # HISTORY predictions
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS public.demand_predictions_history (
                id BIGSERIAL PRIMARY KEY,
                route_id TEXT NOT NULL,
                hour_ts TIMESTAMP WITHOUT TIME ZONE NOT NULL,
                y_true DOUBLE PRECISION,
                y_pred DOUBLE PRECISION,
                run_id UUID NOT NULL,
                model_name TEXT NOT NULL,
                model_version TEXT NOT NULL,
                created_at TIMESTAMP WITHOUT TIME ZONE DEFAULT NOW()
            );
        """))

        conn.execute(text("""
            CREATE INDEX IF NOT EXISTS ix_dph_route_ts
            ON public.demand_predictions_history(route_id, hour_ts);
        """))
        conn.execute(text("""
            CREATE INDEX IF NOT EXISTS ix_dph_run
            ON public.demand_predictions_history(run_id);
        """))

def main():
    ensure_ml_dataset_view()
    ensure_tables()

    df = pd.read_sql("SELECT * FROM public.ml_dataset_hourly ORDER BY hour_ts", engine)
    if df.empty:
        raise SystemExit("ml_dataset_hourly is empty. Need data in passenger_flow_events / trip_updates / vehicle_positions / weather_observations.")

    df["passengers_next_hour"] = df.groupby("route_id")["passengers"].shift(-1)
    df = df.dropna(subset=["passengers_next_hour"]).copy()

    X = df[FEATURES].fillna(0)
    y = df["passengers_next_hour"].astype(float)

    split_idx = int(len(df) * 0.8)
    X_train, X_test = X.iloc[:split_idx], X.iloc[split_idx:]
    y_train, y_test = y.iloc[:split_idx], y.iloc[split_idx:]

    model = RandomForestRegressor(n_estimators=200, random_state=42, n_jobs=-1)
    model.fit(X_train, y_train)

    preds = model.predict(X_test)
    mae = mean_absolute_error(y_test, preds)
    print(f"RUN_ID={RUN_ID} | Rows: {len(df)} | MAE: {mae:.3f}")

    pred_df = df.iloc[split_idx:][["route_id", "hour_ts"]].copy()
    pred_df["y_true"] = y_test.values
    pred_df["y_pred"] = preds
    pred_df["run_id"] = RUN_ID
    pred_df["model_name"] = MODEL_NAME
    pred_df["model_version"] = MODEL_VERSION

    with engine.begin() as conn:
        # ml_runs insert
        conn.execute(text("""
            INSERT INTO public.ml_runs(run_id, model_name, model_version, rows_used, split_ratio, mae, features)
            VALUES (
                CAST(:run_id AS uuid),
                :model_name,
                :model_version,
                :rows_used,
                :split_ratio,
                :mae,
                CAST(:features AS jsonb)
            )
            ON CONFLICT (run_id) DO NOTHING;
        """), {
            "run_id": RUN_ID,
            "model_name": MODEL_NAME,
            "model_version": MODEL_VERSION,
            "rows_used": int(len(df)),
            "split_ratio": 0.8,
            "mae": float(mae),
            "features": json.dumps(FEATURES),
        })

        # HISTORY append
        conn.execute(text("""
            INSERT INTO public.demand_predictions_history
              (route_id, hour_ts, y_true, y_pred, run_id, model_name, model_version)
            VALUES
              (:route_id, :hour_ts, :y_true, :y_pred, CAST(:run_id AS uuid), :model_name, :model_version);
        """), pred_df.to_dict(orient="records"))

        # LATEST upsert
        conn.execute(text("""
            INSERT INTO public.demand_predictions(route_id, hour_ts, y_true, y_pred, run_id, model_name, model_version)
            VALUES (:route_id, :hour_ts, :y_true, :y_pred, CAST(:run_id AS uuid), :model_name, :model_version)
            ON CONFLICT (route_id, hour_ts)
            DO UPDATE SET
              y_true = EXCLUDED.y_true,
              y_pred = EXCLUDED.y_pred,
              run_id = EXCLUDED.run_id,
              model_name = EXCLUDED.model_name,
              model_version = EXCLUDED.model_version,
              created_at = NOW();
        """), pred_df.to_dict(orient="records"))

    print("Saved predictions to demand_predictions (latest) + demand_predictions_history, and logged run in ml_runs.")

if __name__ == "__main__":
    main()
