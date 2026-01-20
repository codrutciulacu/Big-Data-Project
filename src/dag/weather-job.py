from airflow.decorators import dag, task
from datetime import datetime, timedelta, timezone
import os
import requests
import psycopg2
import logging
from airflow.models import Variable


WEATHER_API_URL = "https://api.open-meteo.com/v1/forecast"
WEATHER_API_KEY = Variable.get("WEATHER_API_KEY")

LATITUDE = "44.4268"
LONGITUDE = "26.1025"

DB_HOST = Variable.get("POSTGRES_HOST", "postgis")
DB_PORT = Variable.get("POSTGRES_PORT", "5432")
DB_NAME = Variable.get("POSTGRES_DB", "gtfs")
DB_USER = Variable.get("POSTGRES_USER", "gtfs_user")
DB_PASSWORD = Variable.get("POSTGRES_PASSWORD", "gtfs_password")


@dag(
    start_date=datetime(2021, 1, 1),
    schedule=timedelta(days=1),   # every hour
    catchup=False,
    tags=["weather", "ingestion"]
)
def weather_ingestion_pipeline():

    @task(retries=3, retry_delay=60)
    def fetch_weather():
        params = {
            "latitude": LATITUDE,
            "longitude": LONGITUDE,
            "current": [
                "temperature_2m",
                "precipitation",
                "wind_speed_10m",
                "weather_code"
            ]
        }

        response = requests.get(
            WEATHER_API_URL,
            params=params,
            timeout=10
        )
        response.raise_for_status()
        return response.json()

    @task
    def normalize_weather(api_response: dict):
        current = api_response["current"]

        normalized = {
            "observed_at": datetime.fromisoformat(
                current["time"]
            ).replace(tzinfo=timezone.utc),
            "latitude": LATITUDE,
            "longitude": LONGITUDE,
            "temperature_c": current.get("temperature_2m"),
            "precipitation_mm": current.get("precipitation"),
            "wind_speed_mps": current.get("wind_speed_10m"),
            "weather_code": str(current.get("weather_code"))
        }

        return normalized

    @task
    def write_to_db(weather: dict):
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD
        )

        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO weather_observations (
                        observed_at,
                        latitude,
                        longitude,
                        temperature_c,
                        precipitation_mm,
                        wind_speed_mps,
                        weather_code
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        weather["observed_at"],
                        weather["latitude"],
                        weather["longitude"],
                        weather["temperature_c"],
                        weather["precipitation_mm"],
                        weather["wind_speed_mps"],
                        weather["weather_code"]
                    )
                )

        conn.close()
        logging.info("Weather data inserted successfully")

    api_response = fetch_weather()
    normalized = normalize_weather(api_response)
    write_to_db(normalized)


weather_ingestion_pipeline()