import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from sqlalchemy import create_engine

engine = create_engine("postgresql+psycopg2://gtfs_user:gtfs_password@localhost:5432/gtfs")

df = pd.read_sql("""
  SELECT
    route_id::text AS route_id,
    hour_ts,
    y_pred::double precision AS y_pred,
    y_true::double precision AS y_true
  FROM demand_predictions
  ORDER BY hour_ts, route_id
""", engine)

df["hour_ts"] = pd.to_datetime(df["hour_ts"])
df = df.replace([np.inf, -np.inf], np.nan)

# keep only rows we can validate
val = df.dropna(subset=["y_true", "y_pred"]).copy()

# time features
val["hour_of_day"] = val["hour_ts"].dt.hour
val["is_peak_hour"] = ((val["hour_of_day"].between(7, 9)) | (val["hour_of_day"].between(16, 18))).astype(int)

def mae(y, yhat):
    return float(np.mean(np.abs(y - yhat)))

def rmse(y, yhat):
    return float(np.sqrt(np.mean((y - yhat)**2)))

def smape(y, yhat, eps=1e-6):
    denom = np.maximum((np.abs(y) + np.abs(yhat)), eps)
    return float(np.mean(2.0 * np.abs(y - yhat) / denom) * 100.0)

def bias(y, yhat):
    return float(np.mean(yhat - y))

y = val["y_true"].to_numpy()
yhat = val["y_pred"].to_numpy()

print("\n=== GLOBAL METRICS ===")
print(f"n={len(val)}")
print(f"MAE={mae(y, yhat):.3f}")
print(f"RMSE={rmse(y, yhat):.3f}")
print(f"sMAPE%={smape(y, yhat):.3f}")
print(f"Bias(pred-true)={bias(y, yhat):.3f}")

# breakdowns
val["abs_err"] = (val["y_true"] - val["y_pred"]).abs()
val["err"] = (val["y_pred"] - val["y_true"])

by_hour = val.groupby("hour_of_day").agg(
    n=("y_true", "size"),
    MAE=("abs_err", "mean"),
    Bias=("err", "mean"),
).reset_index()

print("\n=== BY HOUR OF DAY ===")
print(by_hour.to_string(index=False))

by_peak = val.groupby("is_peak_hour").agg(
    n=("y_true", "size"),
    MAE=("abs_err", "mean"),
    Bias=("err", "mean"),
).reset_index()

print("\n=== PEAK VS OFF-PEAK ===")
print(by_peak.to_string(index=False))

by_route = val.groupby("route_id").agg(
    n=("y_true", "size"),
    MAE=("abs_err", "mean"),
    Bias=("err", "mean"),
    mean_true=("y_true", "mean"),
    mean_pred=("y_pred", "mean"),
).reset_index().sort_values("MAE", ascending=False)

print("\n=== BY ROUTE (sorted by MAE) ===")
print(by_route.to_string(index=False))

# plots (optional)
plt.figure()
plt.scatter(val["y_true"], val["y_pred"])
plt.xlabel("Observed (y_true)")
plt.ylabel("Predicted (y_pred)")
plt.title("Predicted vs Observed")
plt.show()

plt.figure()
plt.plot(by_hour["hour_of_day"], by_hour["MAE"])
plt.xlabel("Hour of day")
plt.ylabel("MAE")
plt.title("MAE by hour")
plt.xticks(range(0, 24))
plt.show()
