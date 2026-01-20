from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta

import psycopg2
import random
import json
import math

DB_CFG = dict(
    host="postgis",
    port=5432,
    dbname="gtfs",
    user="gtfs_user",
    password="gtfs_password"
)

TIME_OF_DAY_FACTORS = {
    (6, 9): 1.9,
    (9, 16): 1.0,
    (16, 19): 2.1,
    (19, 23): 0.8,
    (23, 24): 0.4,
    (0, 6): 0.3
}

DAY_OF_WEEK_FACTORS = {
    0: 1.0, 1: 1.0, 2: 1.0, 3: 1.0, 4: 1.0,
    5: 0.75, 6: 0.6
}


EVENTS_PER_HOUR = 100
INTERVAL_MINUTES = 1


def time_factor(hour):
    for (h1, h2), f in TIME_OF_DAY_FACTORS.items():
        if h1 <= hour < h2:
            return f
    return 1.0


def weather_factor(temp, rain):
    if rain >= 5:
        return 1.25
    if rain >= 1:
        return 1.15
    if temp <= -5:
        return 0.85
    if temp >= 30:
        return 0.9
    return 1.0


def delay_factor(delay):
    if delay is None:
        return 1.0
    if delay > 600:
        return 0.75
    if delay > 300:
        return 0.85
    return 1.0


def within_hour_factor(minute: int) -> float:
    """
    Creates peaks within the hour (more passengers around :00 and :30).
    Returns multiplier around ~[0.6 .. 1.4]
    """
    # two bumps centered at 0 and 30 minutes
    bump1 = math.exp(-((minute - 0) ** 2) / (2 * (8 ** 2)))
    bump2 = math.exp(-((minute - 30) ** 2) / (2 * (10 ** 2)))
    return 0.7 + 0.7 * (bump1 + bump2)


@dag(
    start_date=days_ago(1),
    schedule="@hourly",
    catchup=False,
    tags=["mock", "passenger_flow"]
)
def mock_passenger_flow_pipeline():

    @task
    def generate_flow(execution_ts: str):
        """
        Generates multiple passenger_flow_events per stop inside the hour.
        execution_ts should be like: "2026-01-19 15:00:00"
        """
        ts = datetime.strptime(execution_ts, "%Y-%m-%d %H:%M:%S")
        hour = ts.hour
        dow = ts.weekday()

        conn = psycopg2.connect(**DB_CFG)
        cur = conn.cursor()

        # Load stops
        cur.execute("SELECT stop_id, base_weight FROM stop_base_demand")
        stops = cur.fetchall()

        if not stops:
            conn.close()
            raise ValueError("stop_base_demand is empty. Cannot generate passenger flow.")

        # Delay in last hour
        cur.execute(
            """
            SELECT AVG(delay_seconds)
            FROM trip_updates
            WHERE observed_at >= %s - INTERVAL '1 hour'
            """,
            (ts,)
        )
        avg_delay = cur.fetchone()[0]

        # Latest weather
        cur.execute(
            """
            SELECT temperature_c, precipitation_mm
            FROM weather_observations
            ORDER BY observed_at DESC
            LIMIT 1
            """
        )
        weather = cur.fetchone()
        temp, rain = weather if weather else (10, 0)

        tf = time_factor(hour)
        df = DAY_OF_WEEK_FACTORS[dow]
        wf = weather_factor(temp, rain)
        rf = delay_factor(avg_delay)

        rows = []

        for stop_id, base in stops:
            # Base hourly demand for this stop
            stop_noise_hour = random.normalvariate(0, 0.10)
            hourly_demand = base * tf * df * wf * rf * (1 + stop_noise_hour)

            hourly_passengers = max(0, int(round(hourly_demand)))

            # Distribute hourly passengers across 5-minute buckets
            # using within_hour_factor shape + randomness
            weights = []
            bucket_times = []

            for i in range(EVENTS_PER_HOUR):
                bucket_ts = ts + timedelta(minutes=i * INTERVAL_MINUTES)
                m = bucket_ts.minute

                w = within_hour_factor(m)
                w *= (1 + random.normalvariate(0, 0.12))  # per-bucket noise
                w = max(0.05, w)

                weights.append(w)
                bucket_times.append(bucket_ts)

            total_w = sum(weights) if sum(weights) > 0 else 1.0

            # Create passenger count per bucket
            for bucket_ts, w in zip(bucket_times, weights):
                bucket_passengers = int(round(hourly_passengers * (w / total_w)))

                # optional extra tiny jitter
                bucket_passengers = max(0, bucket_passengers)

                rows.append((
                    stop_id,
                    bucket_ts,
                    bucket_passengers,
                    json.dumps({
                        "base": float(base),
                        "time_factor": float(tf),
                        "day_factor": float(df),
                        "weather_factor": float(wf),
                        "delay_factor": float(rf),
                        "within_hour_factor": float(within_hour_factor(bucket_ts.minute)),
                        "hourly_passengers_est": int(hourly_passengers),
                        "interval_minutes": INTERVAL_MINUTES
                    })
                ))

        cur.executemany(
            """
            INSERT INTO passenger_flow_events (
                stop_id, observed_at, estimated_passengers, components
            )
            VALUES (%s, %s, %s, %s)
            ON CONFLICT DO NOTHING
            """,
            rows
        )

        conn.commit()
        conn.close()

        print(f"✅ Inserted {len(rows)} passenger_flow_events rows for hour {ts}")

    # Use data_interval_start (more correct than execution_date.hour string formatting)
    generate_flow("{{ data_interval_start.strftime('%Y-%m-%d %H:00:00') }}")


mock_passenger_flow_pipeline()
