# Route optimisation based on demand

Team matcha - Introduction to Big Data Project

## ML Demand Prediction (Alexandra)

### What it does
- Uses the hourly aggregated dataset (`ml_dataset_hourly` view) built from:
  - passenger demand (mock passenger flow)
  - weather hourly features
  - realtime/service signals (if available)
- Trains a baseline regression model to predict passenger demand per route and hour.
- Writes predictions into PostGIS table: `demand_predictions`.

### Run order (required)
1) `static_gtfs_job` (creates GTFS data + stop_base_demand)
2) `weather_ingestion_pipeline`
3) `mock_passenger_flow_pipeline`
4) ML training + prediction

### How to run ML
```bash
docker compose up -d --build ml
docker exec -it gtfs-ml python src/ml/train_predict.py
