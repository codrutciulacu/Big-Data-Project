- first
```
docker compose up -d --build
docker compose run --rm airflow-init // cred ca dam asta doar daca nu a mai fost deschis inainte
// then trigger runs - http://localhost:8080/login/?next=http%3A%2F%2Flocalhost%3A8080%2Fhome
```

- first run ml
```
docker compose up -d --build ml
docker exec -it gtfs-ml python src/ml/train_predict.py
```

- optimization
```
docker exec -i gtfs-postgis psql -U gtfs_user -d gtfs < src/optimization/kpi_views.sql
docker exec -it gtfs-ml python src/optimization/generate_suggestions.py
docker exec -it gtfs-ml python src/optimization/export_suggestions_json.py
```

- for manual testing
```
docker exec -it gtfs-postgis psql -U gtfs_user -d gtfs
/////// db 
\dv - all tables
\d+ kpi_hourly; - verify view structures and types

SELECT *
FROM kpi_hourly
ORDER BY hour_ts DESC, route_id
LIMIT 10;

// checking if there is enough data for optimization
SELECT
  COUNT(*) AS total_rows,
  COUNT(*) FILTER (WHERE active_vehicles > 0) AS with_vehicles,
  COUNT(*) FILTER (WHERE avg_headway_min IS NOT NULL) AS with_headway,
  COUNT(*) FILTER (
    WHERE active_vehicles > 0 OR avg_headway_min IS NOT NULL
  ) AS usable_for_optimization
FROM kpi_hourly;

// non-optimizable routes due to lack of data
SELECT route_id, hour_ts, active_vehicles, avg_headway_min
FROM kpi_hourly
WHERE active_vehicles = 0
  AND avg_headway_min IS NULL
ORDER BY route_id;

// peak hours (data for just an hour => is normal to not have a peak)
SELECT
  EXTRACT(HOUR FROM hour_ts) AS hour,
  COUNT(*) AS n_routes,
  SUM(is_peak_hour) AS peak_routes
FROM kpi_hourly
GROUP BY 1
ORDER BY 1;

// load distribution
SELECT
  route_id,
  predicted_passengers,
  active_vehicles,
  avg_headway_min
FROM kpi_hourly
WHERE active_vehicles > 0
ORDER BY predicted_passengers DESC
LIMIT 10;

// generated suggestions
SELECT
  action,
  COUNT(*) 
FROM route_optimization_suggestions
GROUP BY action;

SELECT
  route_id,
  hour_ts,
  priority_score,
  reasons->>'rule' AS rule
FROM route_optimization_suggestions
ORDER BY priority_score DESC;

// data for the big json
SELECT
  k.route_id,
  k.hour_ts,
  k.predicted_passengers,
  k.active_vehicles,
  s.action,
  s.priority_score
FROM kpi_hourly k
LEFT JOIN route_optimization_suggestions s
  ON s.route_id = k.route_id::text
 AND s.hour_ts = k.hour_ts
ORDER BY k.route_id;

// latest suggestions route/hour
SELECT *
FROM route_optimization_suggestions_latest
ORDER BY priority_score DESC, hour_ts DESC
LIMIT 50;


// history for route in a day
SELECT route_id, hour_ts, action, current_headway_min, recommended_headway_min, priority_score, model_run_id, created_at
FROM route_optimization_suggestions_history
WHERE route_id = '848'
ORDER BY hour_ts, created_at DESC;

// runs 
SELECT model_run_id, MIN(created_at) AS run_started_at, COUNT(*) AS rows_written
FROM route_optimization_suggestions_history
GROUP BY model_run_id
ORDER BY run_started_at DESC;
```

- testing - not so useful
```
python3 validate_predictions.py
```
