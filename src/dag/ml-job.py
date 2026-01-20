from __future__ import annotations

import os
import uuid
import json
from typing import Dict, List, Any

import pandas as pd
import psycopg2
from psycopg2.extras import execute_batch, execute_values
from sklearn.metrics import mean_absolute_error
from sklearn.ensemble import RandomForestRegressor

from airflow.decorators import dag, task
from airflow.utils.dates import days_ago


# ---------------------------
# Constants / config
# ---------------------------
MODEL_NAME = "rf_next_hour"
MODEL_VERSION = "v1"

FEATURES: List[str] = [
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


def get_pg_conn():
    DB_USER = os.getenv("DB_USER", "gtfs_user")
    DB_PASS = os.getenv("DB_PASS", "gtfs_password")
    DB_HOST = os.getenv("DB_HOST", "postgis")
    DB_PORT = int(os.getenv("DB_PORT", "5432"))
    DB_NAME = os.getenv("DB_NAME", "gtfs")

    return psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASS,
    )


def run_sql_statements(conn, sql: str) -> None:
    """
    Executes multiple SQL statements (split by ';') safely.
    """
    stmts = [s.strip() for s in sql.split(";") if s.strip()]
    with conn.cursor() as cur:
        for stmt in stmts:
            cur.execute(stmt)


@dag(
    dag_id="ml_train_predict_demand_rf_psycopg2",
    start_date=days_ago(1),
    schedule="@hourly",
    catchup=False,
    tags=["ml", "gtfs", "demand", "psycopg2"],
)
def ml_train_predict_demand_rf_psycopg2():
    @task
    def generate_run_metadata() -> Dict[str, str]:
        run_id = str(uuid.uuid4())
        return {
            "run_id": run_id,
            "model_name": MODEL_NAME,
            "model_version": MODEL_VERSION,
        }

    @task
    def ensure_ml_dataset_view() -> None:
        conn = get_pg_conn()
        try:
            conn.autocommit = False
            run_sql_statements(conn, ENSURE_ML_DATASET_VIEW_SQL)
            conn.commit()
        finally:
            conn.close()

    @task
    def ensure_tables() -> None:
        conn = get_pg_conn()
        try:
            conn.autocommit = False
            with conn.cursor() as cur:
                cur.execute(
                    """
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
                    """
                )

                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS public.demand_predictions (
                        route_id TEXT NOT NULL,
                        hour_ts TIMESTAMP WITHOUT TIME ZONE NOT NULL,
                        y_true DOUBLE PRECISION,
                        y_pred DOUBLE PRECISION,
                        created_at TIMESTAMP WITHOUT TIME ZONE DEFAULT NOW(),
                        PRIMARY KEY (route_id, hour_ts)
                    );
                    """
                )

                cur.execute(
                    """
                    ALTER TABLE public.demand_predictions
                      ADD COLUMN IF NOT EXISTS run_id UUID,
                      ADD COLUMN IF NOT EXISTS model_name TEXT,
                      ADD COLUMN IF NOT EXISTS model_version TEXT;
                    """
                )

                cur.execute(
                    """
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
                    """
                )

                cur.execute(
                    """
                    CREATE INDEX IF NOT EXISTS ix_dph_route_ts
                    ON public.demand_predictions_history(route_id, hour_ts);
                    """
                )

                cur.execute(
                    """
                    CREATE INDEX IF NOT EXISTS ix_dph_run
                    ON public.demand_predictions_history(run_id);
                    """
                )

            conn.commit()
        finally:
            conn.close()

    @task
    def load_dataset() -> pd.DataFrame:
        conn = get_pg_conn()
        try:
            df = pd.read_sql_query(
                "SELECT * FROM public.ml_dataset_hourly ORDER BY hour_ts",
                conn,
            )
        finally:
            conn.close()

        if df.empty:
            raise ValueError(
                "ml_dataset_hourly is empty. Need data in passenger_flow_events / trip_updates / "
                "vehicle_positions / weather_observations."
            )
        return df

    @task
    def train_and_prepare_predictions(df: pd.DataFrame, meta: Dict[str, str]) -> Dict[str, Any]:
        df = df.copy()
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

        print(f"RUN_ID={meta['run_id']} | Rows: {len(df)} | MAE: {mae:.3f}")

        pred_df = df.iloc[split_idx:][["route_id", "hour_ts"]].copy()
        pred_df["y_true"] = y_test.values
        pred_df["y_pred"] = preds

        # keep XCom smaller (dict records) - note: still can be big
        pred_records = []
        for row in pred_df.itertuples(index=False):
            pred_records.append(
                {
                    "route_id": row.route_id,
                    "hour_ts": row.hour_ts.to_pydatetime().isoformat(),
                    "y_true": float(row.y_true),
                    "y_pred": float(row.y_pred),
                    "run_id": meta["run_id"],
                    "model_name": meta["model_name"],
                    "model_version": meta["model_version"],
                }
            )

        return {
            "run_id": meta["run_id"],
            "model_name": meta["model_name"],
            "model_version": meta["model_version"],
            "rows_used": int(len(df)),
            "split_ratio": 0.8,
            "mae": float(mae),
            "features": FEATURES,
            "predictions": pred_records,
        }

    @task
    def save_results(result: Dict[str, Any]) -> None:
        conn = get_pg_conn()
        try:
            conn.autocommit = False

            with conn.cursor() as cur:
                # ml_runs
                cur.execute(
                    """
                    INSERT INTO public.ml_runs(run_id, model_name, model_version, rows_used, split_ratio, mae, features)
                    VALUES (%s::uuid, %s, %s, %s, %s, %s, %s::jsonb)
                    ON CONFLICT (run_id) DO NOTHING;
                    """,
                    (
                        result["run_id"],
                        result["model_name"],
                        result["model_version"],
                        result["rows_used"],
                        result["split_ratio"],
                        result["mae"],
                        json.dumps(result["features"]),
                    ),
                )

                preds = result["predictions"]

                # HISTORY append (fast bulk insert)
                history_rows = [
                    (
                        p["route_id"],
                        p["hour_ts"],
                        p["y_true"],
                        p["y_pred"],
                        p["run_id"],
                        p["model_name"],
                        p["model_version"],
                    )
                    for p in preds
                ]

                execute_values(
                    cur,
                    """
                    INSERT INTO public.demand_predictions_history
                      (route_id, hour_ts, y_true, y_pred, run_id, model_name, model_version)
                    VALUES %s;
                    """,
                    history_rows,
                    page_size=2000,
                )

                # LATEST upsert
                execute_values(
                    cur,
                    """
                    INSERT INTO public.demand_predictions
                      (route_id, hour_ts, y_true, y_pred, run_id, model_name, model_version)
                    VALUES %s
                    ON CONFLICT (route_id, hour_ts)
                    DO UPDATE SET
                      y_true = EXCLUDED.y_true,
                      y_pred = EXCLUDED.y_pred,
                      run_id = EXCLUDED.run_id,
                      model_name = EXCLUDED.model_name,
                      model_version = EXCLUDED.model_version,
                      created_at = NOW();
                    """,
                    history_rows,
                    page_size=2000,
                )

            conn.commit()
            print(
                "Saved predictions to demand_predictions (latest) + demand_predictions_history, "
                "and logged run in ml_runs."
            )
        finally:
            conn.close()

    # ---------------------------
    # DAG wiring
    # ---------------------------
    meta = generate_run_metadata()

    ensure_ml_dataset_view()
    ensure_tables()

    df = load_dataset()
    result = train_and_prepare_predictions(df=df, meta=meta)
    save_results(result)


ml_train_predict_demand_rf_psycopg2()
