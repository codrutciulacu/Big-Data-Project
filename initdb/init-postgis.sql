CREATE EXTENSION IF NOT EXISTS postgis;
CREATE EXTENSION IF NOT EXISTS postgis_topology;
CREATE EXTENSION IF NOT EXISTS btree_gist;
CREATE TABLE IF NOT EXISTS gtfs_versions (
    version_id    SERIAL PRIMARY KEY,
    checksum      TEXT NOT NULL,
    source_url    TEXT,
    valid_from    DATE,
    valid_to      DATE,
    loaded_at     TIMESTAMP DEFAULT now()
);

CREATE TABLE IF NOT EXISTS stops (
    stop_id     TEXT NOT NULL,
    stop_name   TEXT,
    stop_lat    DOUBLE PRECISION NOT NULL,
    stop_lon    DOUBLE PRECISION NOT NULL,
    geom        GEOMETRY(Point, 4326) NOT NULL,
    version_id  INT NOT NULL,

    PRIMARY KEY (stop_id, version_id),
    FOREIGN KEY (version_id)
        REFERENCES gtfs_versions(version_id)
);

CREATE INDEX IF NOT EXISTS idx_stops_geom
ON stops USING GIST (geom);

CREATE TABLE IF NOT EXISTS routes (
    route_id          TEXT NOT NULL,
    route_short_name  TEXT,
    route_long_name   TEXT,
    route_type        INT,
    version_id        INT NOT NULL,

    PRIMARY KEY (route_id, version_id),
    FOREIGN KEY (version_id)
        REFERENCES gtfs_versions(version_id)
);

CREATE TABLE IF NOT EXISTS trips (
    trip_id       TEXT NOT NULL,
    route_id      TEXT NOT NULL,
    service_id    TEXT NOT NULL,
    direction_id  INT,
    version_id    INT NOT NULL,

    PRIMARY KEY (trip_id, version_id),
    FOREIGN KEY (route_id, version_id)
        REFERENCES routes(route_id, version_id),
    FOREIGN KEY (version_id)
        REFERENCES gtfs_versions(version_id)
);

CREATE TABLE IF NOT EXISTS calendar (
    service_id  TEXT NOT NULL,
    monday      BOOLEAN,
    tuesday     BOOLEAN,
    wednesday   BOOLEAN,
    thursday    BOOLEAN,
    friday      BOOLEAN,
    saturday    BOOLEAN,
    sunday      BOOLEAN,
    start_date  DATE,
    end_date    DATE,
    version_id  INT NOT NULL,

    PRIMARY KEY (service_id, version_id),
    FOREIGN KEY (version_id)
        REFERENCES gtfs_versions(version_id)
);

CREATE TABLE IF NOT EXISTS stop_times (
    trip_id        TEXT NOT NULL,
    stop_id        TEXT NOT NULL,
    stop_sequence  INT NOT NULL,
    arrival_time   INTEGER,
    departure_time INTEGER,
    version_id     INT NOT NULL,

    PRIMARY KEY (trip_id, stop_sequence, version_id),
    FOREIGN KEY (trip_id, version_id)
        REFERENCES trips(trip_id, version_id),
    FOREIGN KEY (stop_id, version_id)
        REFERENCES stops(stop_id, version_id),
    FOREIGN KEY (version_id)
        REFERENCES gtfs_versions(version_id)
);
CREATE INDEX IF NOT EXISTS idx_stop_times_trip
ON stop_times (trip_id, version_id);


CREATE TABLE IF NOT EXISTS weather_observations (
    id SERIAL PRIMARY KEY,
    observed_at TIMESTAMP NOT NULL,
    latitude DOUBLE PRECISION NOT NULL,
    longitude DOUBLE PRECISION NOT NULL,
    temperature_c DOUBLE PRECISION,
    precipitation_mm DOUBLE PRECISION,
    wind_speed_mps DOUBLE PRECISION,
    weather_code TEXT,
    created_at TIMESTAMP DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_weather_time
ON weather_observations (observed_at);

CREATE TABLE IF NOT EXISTS vehicle_positions (
    vehicle_id   TEXT NOT NULL,
    trip_id      TEXT,
    route_id     TEXT,
    lat          DOUBLE PRECISION NOT NULL,
    lon          DOUBLE PRECISION NOT NULL,
    geom         GEOMETRY(Point, 4326) NOT NULL,
    observed_at  TIMESTAMP NOT NULL,
    ingested_at  TIMESTAMP NOT NULL DEFAULT now(),

    PRIMARY KEY (vehicle_id, observed_at)
);

CREATE INDEX IF NOT EXISTS idx_vehicle_positions_geom
ON vehicle_positions
USING GIST (geom);

CREATE INDEX IF NOT EXISTS idx_vehicle_positions_observed_at
ON vehicle_positions (observed_at);

CREATE TABLE trip_updates (
  trip_id        TEXT,
  route_id       TEXT,
  delay_seconds  INT,
  observed_at    TIMESTAMP,
  ingested_at    TIMESTAMP,
  PRIMARY KEY (trip_id, observed_at)
);

CREATE TABLE service_alerts (
  alert_id     TEXT,
  alert_type   TEXT,
  description  TEXT,
  observed_at  TIMESTAMP,
  ingested_at  TIMESTAMP,
  PRIMARY KEY (alert_id, observed_at)
);

CREATE INDEX IF NOT EXISTS idx_vehicle_positions_route
ON vehicle_positions (route_id);

CREATE INDEX IF NOT EXISTS idx_vehicle_positions_trip
ON vehicle_positions (trip_id);

CREATE TABLE IF NOT EXISTS stop_base_demand (
  stop_id TEXT NOT NULL,
  version_id INTEGER NOT NULL,
  base_weight DOUBLE PRECISION NOT NULL,
  PRIMARY KEY (stop_id, version_id),
  FOREIGN KEY (version_id) REFERENCES gtfs_versions(version_id) ON DELETE CASCADE
);


CREATE TABLE IF NOT EXISTS passenger_flow_events (
    stop_id TEXT NOT NULL,
    observed_at TIMESTAMP NOT NULL,
    estimated_passengers INT NOT NULL,
    components JSONB NOT NULL,
    source TEXT DEFAULT 'mock',
    PRIMARY KEY (stop_id, observed_at)
);

INSERT INTO stop_base_demand (stop_id, base_weight)
SELECT
    stop_id,
    LEAST(50, GREATEST(5, COUNT(*) * 3))
FROM stop_times
GROUP BY stop_id
ON CONFLICT DO NOTHING;

-- -----------------------
-- Views for ML dataset
-- -----------------------

CREATE OR REPLACE VIEW stop_to_route AS
SELECT DISTINCT st.stop_id, t.route_id
FROM stop_times st
JOIN trips t ON t.trip_id = st.trip_id;

CREATE OR REPLACE VIEW passenger_demand_hourly AS
SELECT
  m.route_id,
  date_trunc('hour', p.observed_at) AS hour_ts,
  SUM(p.estimated_passengers)::double precision AS passengers
FROM passenger_flow_events p
JOIN stop_to_route m ON m.stop_id = p.stop_id
GROUP BY m.route_id, date_trunc('hour', p.observed_at);

CREATE OR REPLACE VIEW delay_hourly AS
SELECT
  route_id,
  date_trunc('hour', observed_at) AS hour_ts,
  COUNT(*) AS trip_update_events,
  AVG(delay_seconds)::double precision AS avg_delay_seconds
FROM trip_updates
GROUP BY route_id, date_trunc('hour', observed_at);

CREATE OR REPLACE VIEW vehicle_hourly AS
SELECT
  route_id,
  date_trunc('hour', observed_at) AS hour_ts,
  COUNT(*) AS vehicle_events
FROM vehicle_positions
GROUP BY route_id, date_trunc('hour', observed_at);

CREATE OR REPLACE VIEW weather_hourly AS
SELECT
  date_trunc('hour', observed_at) AS hour_ts,
  AVG(temperature_c)::double precision AS temperature_c,
  AVG(precipitation_mm)::double precision AS precipitation_mm,
  AVG(wind_speed_mps)::double precision AS wind_speed_mps
FROM weather_observations
GROUP BY date_trunc('hour', observed_at);

CREATE OR REPLACE VIEW ml_dataset_hourly AS
SELECT
  pd.route_id,
  pd.hour_ts,
  pd.passengers,

  COALESCE(dh.trip_update_events, 0) AS trip_update_events,
  COALESCE(dh.avg_delay_seconds, 0) AS avg_delay_seconds,
  COALESCE(vh.vehicle_events, 0) AS vehicle_events,

  COALESCE(wh.temperature_c, 0) AS temperature_c,
  COALESCE(wh.precipitation_mm, 0) AS precipitation_mm,
  COALESCE(wh.wind_speed_mps, 0) AS wind_speed_mps,

  EXTRACT(HOUR FROM pd.hour_ts) AS hour,
  EXTRACT(DOW FROM pd.hour_ts) AS day_of_week,
  (EXTRACT(HOUR FROM pd.hour_ts) BETWEEN 7 AND 9
   OR EXTRACT(HOUR FROM pd.hour_ts) BETWEEN 16 AND 18)::int AS is_peak
FROM passenger_demand_hourly pd
LEFT JOIN delay_hourly dh
  ON pd.route_id = dh.route_id AND pd.hour_ts = dh.hour_ts
LEFT JOIN vehicle_hourly vh
  ON pd.route_id = vh.route_id AND pd.hour_ts = vh.hour_ts
LEFT JOIN weather_hourly wh
  ON pd.hour_ts = wh.hour_ts;
