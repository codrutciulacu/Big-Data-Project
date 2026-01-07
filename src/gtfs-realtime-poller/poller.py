import os
import time
import json
import logging
import requests
from datetime import datetime, timezone

from confluent_kafka import Producer
from google.transit import gtfs_realtime_pb2

GTFS_RT_VEHICLE_POSITIONS_URL = os.getenv(
    "GTFS_RT_VEHICLE_POSITIONS_URL",
    "https://gtfs.tpbi.ro/api/gtfs-rt/vehiclePositions"
)

GTFS_RT_TRIP_UPDATES_URL = os.getenv(
    "GTFS_RT_TRIP_UPDATES_URL",
    "https://gtfs.tpbi.ro/api/gtfs-rt/tripUpdates"
)

GTFS_RT_ALERTS_URL = os.getenv(
    "GTFS_RT_ALERTS_URL",
    "https://gtfs.tpbi.ro/api/gtfs-rt/serviceAlerts"
)

POLL_INTERVAL = int(os.getenv("POLL_INTERVAL", "10"))

KAFKA_BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS", "localhost:29092"
)

TOPIC_VEHICLE_POSITIONS = "gtfs.rt.vehicle_positions"
TOPIC_TRIP_UPDATES = "gtfs.rt.trip_updates"
TOPIC_ALERTS = "gtfs.rt.alerts"


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

def create_producer():
    return Producer({
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "linger.ms": 5
    })


def send_to_kafka(producer, topic, key, value):
    producer.produce(
        topic=topic,
        key=key,
        value=json.dumps(value)
    )

def now_iso():
    return datetime.now(timezone.utc).isoformat()


def valid_lat_lon(lat, lon):
    return (
        lat is not None and lon is not None and
        -90 <= lat <= 90 and
        -180 <= lon <= 180
    )


def fetch_feed(url):
    feed = gtfs_realtime_pb2.FeedMessage()
    response = requests.get(url, timeout=10)
    response.raise_for_status()
    feed.ParseFromString(response.content)
    return feed

def process_vehicle_positions(feed, producer):
    for entity in feed.entity:
        if not entity.HasField("vehicle"):
            continue

        v = entity.vehicle

        if not (
            v.vehicle and v.vehicle.id and
            v.position and
            valid_lat_lon(v.position.latitude, v.position.longitude)
        ):
            continue

        event = {
            "event_type": "vehicle_position",
            "entity_id": v.vehicle.id,
            "timestamp": (
                datetime.fromtimestamp(v.timestamp, timezone.utc).isoformat()
                if v.timestamp else now_iso()
            ),
            "ingested_at": now_iso(),
            "source": "gtfs-realtime",
            "payload": {
                "vehicle_id": v.vehicle.id,
                "trip_id": v.trip.trip_id if v.trip else None,
                "route_id": v.trip.route_id if v.trip else None,
                "latitude": v.position.latitude,
                "longitude": v.position.longitude,
                "bearing": v.position.bearing
                if v.position.HasField("bearing") else None,
                "speed": v.position.speed
                if v.position.HasField("speed") else None
            }
        }

        send_to_kafka(
            producer,
            TOPIC_VEHICLE_POSITIONS,
            key=event["entity_id"],
            value=event
        )

def process_trip_updates(feed, producer):
    for entity in feed.entity:
        if not entity.HasField("trip_update"):
            continue

        t = entity.trip_update

        if not t.trip or not t.trip.trip_id:
            continue

        stop_updates = []
        for stu in t.stop_time_update:
            stop_updates.append({
                "stop_id": stu.stop_id,
                "arrival_delay": stu.arrival.delay
                if stu.HasField("arrival") else None,
                "departure_delay": stu.departure.delay
                if stu.HasField("departure") else None
            })

        event = {
            "event_type": "trip_update",
            "entity_id": t.trip.trip_id,
            "timestamp": now_iso(),
            "ingested_at": now_iso(),
            "source": "gtfs-realtime",
            "payload": {
                "trip_id": t.trip.trip_id,
                "route_id": t.trip.route_id,
                "delay_seconds": t.delay,
                "stop_updates": stop_updates
            }
        }

        send_to_kafka(
            producer,
            TOPIC_TRIP_UPDATES,
            key=event["entity_id"],
            value=event
        )

def process_alerts(feed, producer):
    for entity in feed.entity:
        if not entity.HasField("alert"):
            continue

        a = entity.alert
        alert_id = entity.id or "unknown"

        event = {
            "event_type": "service_alert",
            "entity_id": alert_id,
            "timestamp": now_iso(),
            "ingested_at": now_iso(),
            "source": "gtfs-realtime",
            "payload": {
                "alert_id": alert_id,
                "cause": a.cause,
                "effect": a.effect,
                "description": (
                    a.header_text.translation[0].text
                    if a.header_text.translation else None
                )
            }
        }

        send_to_kafka(
            producer,
            TOPIC_ALERTS,
            key=event["entity_id"],
            value=event
        )

def main():
    producer = create_producer()

    logging.info("Starting GTFS-RT poller (3 endpoints)")

    while True:
        try:
            process_vehicle_positions(
                fetch_feed(GTFS_RT_VEHICLE_POSITIONS_URL),
                producer
            )

            process_trip_updates(
                fetch_feed(GTFS_RT_TRIP_UPDATES_URL),
                producer
            )

            process_alerts(
                fetch_feed(GTFS_RT_ALERTS_URL),
                producer
            )

            producer.poll(0)

        except Exception:
            logging.exception("Error during GTFS-RT polling")

        time.sleep(POLL_INTERVAL)


if __name__ == "__main__":
    main()
