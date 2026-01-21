-- =========================
-- KPI VIEWS (hourly) - FINAL (SAFE AFTER ML)
-- =========================
-- IMPORTANT:
-- - KPI views use kpi_* names to avoid colliding with ML views (ml_dataset_hourly depends on delay_hourly)
-- - NO CASCADE on drops (safe after ML)
-- - DROP + CREATE (not OR REPLACE) to avoid view column type conflicts

-- Clean rebuild (safe)
DROP VIEW IF EXISTS public.route_optimization_suggestions;        -- alias for export script
DROP VIEW IF EXISTS public.route_optimization_suggestions_latest;
DROP VIEW IF EXISTS public.kpi_hourly;

DROP VIEW IF EXISTS public.kpi_delay_hourly;
DROP VIEW IF EXISTS public.kpi_headway_hourly;
DROP VIEW IF EXISTS public.kpi_active_vehicles_hourly;

-- 1) Active vehicles per route/hour
CREATE VIEW public.kpi_active_vehicles_hourly AS
SELECT
  route_id::text AS route_id,
  date_trunc('hour', observed_at) AS hour_ts,
  COUNT(DISTINCT vehicle_id) AS active_vehicles
FROM public.vehicle_positions
WHERE route_id IS NOT NULL
GROUP BY route_id::text, date_trunc('hour', observed_at);

-- 2) Headway estimate (minutes)
CREATE VIEW public.kpi_headway_hourly AS
WITH vehicle_first_seen AS (
  SELECT
    route_id::text AS route_id,
    date_trunc('hour', observed_at) AS hour_ts,
    vehicle_id,
    MIN(observed_at) AS first_seen
  FROM public.vehicle_positions
  WHERE route_id IS NOT NULL
  GROUP BY route_id::text, date_trunc('hour', observed_at), vehicle_id
),
ordered AS (
  SELECT
    route_id,
    hour_ts,
    first_seen,
    EXTRACT(EPOCH FROM (first_seen - LAG(first_seen) OVER (
      PARTITION BY route_id, hour_ts ORDER BY first_seen
    ))) / 60.0 AS headway_min
  FROM vehicle_first_seen
)
SELECT
  route_id,
  hour_ts,
  AVG(headway_min) FILTER (
    WHERE headway_min IS NOT NULL AND headway_min BETWEEN 0.5 AND 60
  ) AS avg_headway_min
FROM ordered
GROUP BY route_id, hour_ts;

-- 3) Delay hourly
CREATE VIEW public.kpi_delay_hourly AS
SELECT
  route_id::text AS route_id,
  date_trunc('hour', observed_at) AS hour_ts,
  COUNT(*) AS trip_update_events,
  AVG(delay_seconds)::double precision AS avg_delay_seconds
FROM public.trip_updates
WHERE route_id IS NOT NULL
GROUP BY route_id::text, date_trunc('hour', observed_at);

-- 4) KPI hourly (predictions + ops KPIs)
CREATE VIEW public.kpi_hourly AS
SELECT
  d.route_id::text AS route_id,
  d.hour_ts,
  d.y_pred AS predicted_passengers,
  d.y_true AS observed_passengers,

  COALESCE(av.active_vehicles, 0) AS active_vehicles,
  h.avg_headway_min,

  COALESCE(dl.trip_update_events, 0) AS trip_update_events,
  COALESCE(dl.avg_delay_seconds, 0)::double precision AS avg_delay_seconds,

  EXTRACT(HOUR FROM d.hour_ts)::int AS hour_of_day,
  (
    (EXTRACT(HOUR FROM d.hour_ts) BETWEEN 7 AND 9)
    OR (EXTRACT(HOUR FROM d.hour_ts) BETWEEN 16 AND 18)
  )::int AS is_peak_hour
FROM public.demand_predictions d
LEFT JOIN public.kpi_active_vehicles_hourly av
  ON d.route_id::text = av.route_id::text
 AND d.hour_ts = av.hour_ts
LEFT JOIN public.kpi_headway_hourly h
  ON d.route_id::text = h.route_id::text
 AND d.hour_ts = h.hour_ts
LEFT JOIN public.kpi_delay_hourly dl
  ON d.route_id::text = dl.route_id::text
 AND d.hour_ts = dl.hour_ts;

-- =========================
-- SUGGESTIONS PERSISTENCE
-- =========================

CREATE TABLE IF NOT EXISTS public.route_optimization_suggestions_history (
  id BIGSERIAL PRIMARY KEY,
  route_id TEXT NOT NULL,
  hour_ts TIMESTAMP WITHOUT TIME ZONE NOT NULL,
  action TEXT NOT NULL,
  current_headway_min DOUBLE PRECISION,
  recommended_headway_min DOUBLE PRECISION,
  priority_score DOUBLE PRECISION NOT NULL,
  reasons JSONB NOT NULL,
  model_run_id UUID NOT NULL,
  created_at TIMESTAMP WITHOUT TIME ZONE DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_rosh_route_hour
  ON public.route_optimization_suggestions_history(route_id, hour_ts);

CREATE INDEX IF NOT EXISTS idx_rosh_created
  ON public.route_optimization_suggestions_history(created_at DESC);

CREATE INDEX IF NOT EXISTS idx_rosh_run
  ON public.route_optimization_suggestions_history(model_run_id);

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint WHERE conname = 'uq_rosh_route_hour_run'
  ) THEN
    ALTER TABLE public.route_optimization_suggestions_history
    ADD CONSTRAINT uq_rosh_route_hour_run UNIQUE (route_id, hour_ts, model_run_id);
  END IF;
END $$;

CREATE VIEW public.route_optimization_suggestions_latest AS
SELECT DISTINCT ON (route_id, hour_ts)
  route_id,
  hour_ts,
  action,
  current_headway_min,
  recommended_headway_min,
  priority_score,
  reasons,
  model_run_id,
  created_at
FROM public.route_optimization_suggestions_history
ORDER BY route_id, hour_ts, created_at DESC;

-- Alias for older scripts (export_suggestions_json.py expects this name)
CREATE VIEW public.route_optimization_suggestions AS
SELECT *
FROM public.route_optimization_suggestions_latest;
