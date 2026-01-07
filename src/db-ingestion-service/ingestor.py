import os
import json
import logging
from datetime import datetime
from confluent_kafka import Consumer
import psycopg2

KAFKA_BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS", "localhost:29092"
)

KAFKA_GROUP_ID = os.getenv(
    "KAFKA_GROUP_ID", "gtfs-rt-db-consumer"
)

KAFKA_TOPICS = [
    "gtfs.rt.vehicle_positions",
    "gtfs.rt.trip_updates",
    "gtfs.rt.alerts"
]

DB_HOST = os.getenv("POSTGRES_HOST", "localhost")
DB_PORT = os.getenv("POSTGRES_PORT", "5432")
DB_NAME = os.getenv("POSTGRES_DB", "gtfs")
DB_USER = os.getenv("POSTGRES_USER", "gtfs_user")
DB_PASSWORD = os.getenv("POSTGRES_PASSWORD", "gtfs_password")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

def create_consumer():
    return Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "group.id": KAFKA_GROUP_ID,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False
    })

def get_db_conn():
    return psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD
    )

def insert_vehicle_position(cur, event):
    p = event["payload"]

    cur.execute(
        """
        INSERT INTO vehicle_positions (
            vehicle_id,
            trip_id,
            route_id,
            lat,
            lon,
            geom,
            observed_at,
            ingested_at
        )
        VALUES (
            %s, %s, %s,
            %s, %s,
            ST_SetSRID(ST_MakePoint(%s, %s), 4326),
            %s, %s
        )
        ON CONFLICT (vehicle_id, observed_at) DO NOTHING
        """,
        (
            p["vehicle_id"],
            p.get("trip_id"),
            p.get("route_id"),
            p["latitude"],
            p["longitude"],
            p["longitude"],   # lon first
            p["latitude"],    # lat second
            event["timestamp"],
            event["ingested_at"]
        )
    )


def insert_trip_update(cur, event):
    p = event["payload"]

    cur.execute(
        """
        INSERT INTO trip_updates (
            trip_id,
            route_id,
            delay_seconds,
            observed_at,
            ingested_at
        )
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (trip_id, observed_at)
        DO UPDATE SET
            delay_seconds = EXCLUDED.delay_seconds,
            ingested_at = EXCLUDED.ingested_at;
        """,
        (
            p["trip_id"],
            p.get("route_id"),
            p.get("delay_seconds"),
            event["timestamp"],
            event["ingested_at"]
        )
    )


def insert_alert(cur, event):
    p = event["payload"]

    cur.execute(
        """
        INSERT INTO service_alerts (
            alert_id,
            alert_type,
            description,
            observed_at,
            ingested_at
        )
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (alert_id, observed_at)
        DO UPDATE SET
            alert_type = EXCLUDED.alert_type,
            description = EXCLUDED.description,
            observed_at = EXCLUDED.observed_at,
            ingested_at = EXCLUDED.ingested_at;
        """,
        (
            p["alert_id"],
            p.get("alert_type"),
            p.get("description"),
            event["timestamp"],
            event["ingested_at"]
        )
    )


def handle_event(cur, event):
    event_type = event.get("event_type")

    if event_type == "vehicle_position":
        insert_vehicle_position(cur, event)

    elif event_type == "trip_update":
        insert_trip_update(cur, event)

    elif event_type == "service_alert":
        insert_alert(cur, event)

    else:
        logging.warning("Unknown event type: %s", event_type)

def main():
    consumer = create_consumer()
    consumer.subscribe(KAFKA_TOPICS)

    conn = get_db_conn()
    conn.autocommit = False

    logging.info("GTFS-RT DB consumer started")

    try:
        while True:
            msg = consumer.poll(timeout=1.0)

            if msg is None:
                continue

            if msg.error():
                logging.error("Kafka error: %s", msg.error())
                continue

            try:
                event = json.loads(msg.value().decode("utf-8"))

                with conn.cursor() as cur:
                    handle_event(cur, event)

                conn.commit()
                consumer.commit(msg)

            except Exception:
                conn.rollback()
                logging.exception("Failed to process message")

    finally:
        consumer.close()
        conn.close()


if __name__ == "__main__":
    main()
