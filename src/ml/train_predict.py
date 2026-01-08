import pandas as pd
from sqlalchemy import create_engine, text
from sklearn.metrics import mean_absolute_error
from sklearn.ensemble import RandomForestRegressor

DB_USER = "gtfs_user"
DB_PASS = "gtfs_password"
DB_HOST = "localhost"
DB_PORT = 5432
DB_NAME = "gtfs"

engine = create_engine(
    "postgresql+psycopg2://gtfs_user:gtfs_password@postgis:5432/gtfs"
)

# Load dataset
df = pd.read_sql("SELECT * FROM ml_dataset_hourly ORDER BY hour_ts", engine)

# Target: next hour passengers per route
df["passengers_next_hour"] = df.groupby("route_id")["passengers"].shift(-1)
df = df.dropna(subset=["passengers_next_hour"]).copy()

features = [
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

X = df[features].fillna(0)
y = df["passengers_next_hour"].astype(float)

# Time-based split (80/20, no shuffle)
split_idx = int(len(df) * 0.8)
X_train, X_test = X.iloc[:split_idx], X.iloc[split_idx:]
y_train, y_test = y.iloc[:split_idx], y.iloc[split_idx:]

# Train model
model = RandomForestRegressor(n_estimators=200, random_state=42, n_jobs=-1)
model.fit(X_train, y_train)

# Evaluate
preds = model.predict(X_test)
mae = mean_absolute_error(y_test, preds)
print(f"Rows: {len(df)} | MAE: {mae:.3f}")

# Save predictions to DB
pred_df = df.iloc[split_idx:][["route_id", "hour_ts"]].copy()
pred_df["y_true"] = y_test.values
pred_df["y_pred"] = preds

with engine.begin() as conn:
    conn.execute(text("""
        CREATE TABLE IF NOT EXISTS demand_predictions (
            route_id TEXT NOT NULL,
            hour_ts TIMESTAMP WITHOUT TIME ZONE NOT NULL,
            y_true DOUBLE PRECISION,
            y_pred DOUBLE PRECISION,
            created_at TIMESTAMP WITHOUT TIME ZONE DEFAULT NOW(),
            PRIMARY KEY (route_id, hour_ts)
        );
    """))

    conn.execute(text("""
        INSERT INTO demand_predictions(route_id, hour_ts, y_true, y_pred)
        VALUES (:route_id, :hour_ts, :y_true, :y_pred)
        ON CONFLICT (route_id, hour_ts)
        DO UPDATE SET
          y_true = EXCLUDED.y_true,
          y_pred = EXCLUDED.y_pred,
          created_at = NOW();
    """), pred_df.to_dict(orient="records"))

print("Saved predictions to demand_predictions.")
