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
    arrival_time   TIME,
    departure_time TIME,
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