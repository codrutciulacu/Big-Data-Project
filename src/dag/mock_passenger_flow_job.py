from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
import psycopg2
import random
import json
import math
from datetime import datetime

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

@dag(
    start_date=days_ago(1),
    schedule="0 * * * *",  # hourly
    catchup=False,
    tags=["mock", "passenger_flow"]
)
def mock_passenger_flow_pipeline():

    @task
    def generate_flow(generated_date: str):
        generated_date_fixed = generated_date.replace("T", " ")
        ts = datetime.strptime(generated_date_fixed, "%Y-%m-%d %H:%M:%S")
        hour = ts.hour
        dow = ts.weekday()

        conn = psycopg2.connect(**DB_CFG)
        cur = conn.cursor()

        cur.execute("SELECT stop_id, base_weight FROM stop_base_demand")
        stops = cur.fetchall()

        cur.execute(
            """
            SELECT AVG(delay_seconds)
            FROM trip_updates
            WHERE observed_at >= %s - INTERVAL '1 hour'
            """,
            (ts,)
        )
        avg_delay = cur.fetchone()[0]

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
            noise = random.normalvariate(0, 0.15)
            demand = base * tf * df * wf * rf * (1 + noise)
            passengers = max(0, int(round(demand)))

            rows.append((
                stop_id,
                ts,
                passengers,
                json.dumps({
                    "base": base,
                    "time_factor": tf,
                    "day_factor": df,
                    "weather_factor": wf,
                    "delay_factor": rf
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

    generate_flow("{{ ds }}T{{ execution_date.hour }}:00:00")


mock_passenger_flow_pipeline()
