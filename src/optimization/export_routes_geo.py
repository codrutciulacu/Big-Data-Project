#!/usr/bin/env python3
import os
import json
import psycopg2
from psycopg2.extras import RealDictCursor

# Database connection
running_in_docker = os.getenv("RUNNING_IN_DOCKER", "0") == "1"
DB_HOST = os.getenv("DB_HOST", "postgis" if running_in_docker else "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "gtfs")
DB_USER = os.getenv("DB_USER", "gtfs_user")
DB_PASS = os.getenv("DB_PASS", "gtfs_password")

conn = psycopg2.connect(
    host=DB_HOST,
    port=DB_PORT,
    dbname=DB_NAME,
    user=DB_USER,
    password=DB_PASS,
    cursor_factory=RealDictCursor
)

# Get ALL routes with their stops from the database
query = """
SELECT DISTINCT
    r.route_id,
    r.route_short_name,
    r.route_long_name,
    s.stop_id,
    s.stop_name,
    s.stop_lat,
    s.stop_lon
FROM routes r
JOIN trips t ON t.route_id = r.route_id
JOIN stop_times st ON st.trip_id = t.trip_id
JOIN stops s ON s.stop_id = st.stop_id
WHERE s.stop_lat IS NOT NULL 
  AND s.stop_lon IS NOT NULL
ORDER BY r.route_id
LIMIT 1000;
"""

with conn.cursor() as cur:
    cur.execute(query)
    rows = cur.fetchall()

# Group stops by route
routes_map = {}
for row in rows:
    route_id = row['route_id']
    if route_id not in routes_map:
        routes_map[route_id] = {
            'route_id': route_id,
            'route_short_name': row['route_short_name'],
            'route_long_name': row['route_long_name'],
            'stops': []
        }
    
    routes_map[route_id]['stops'].append({
        'stop_id': row['stop_id'],
        'stop_name': row['stop_name'],
        'lat': float(row['stop_lat']),
        'lon': float(row['stop_lon'])
    })

routes_list = list(routes_map.values())

# Write to JSON - both in exports/ and frontend/public/exports/
output_path = 'exports/routes_geo_latest.json'
os.makedirs('exports', exist_ok=True)

with open(output_path, 'w', encoding='utf-8') as f:
    json.dump(routes_list, f, ensure_ascii=False, indent=2)

# Also write to frontend for automatic sync
frontend_path = 'exports/routes_geo_latest.json'
if os.path.exists('exports'):
    with open(frontend_path, 'w', encoding='utf-8') as f:
        json.dump(routes_list, f, ensure_ascii=False, indent=2)
    print(f"✅ Exported {len(routes_list)} routes to:")
    print(f"   - {output_path}")
    print(f"   - {frontend_path}")
else:
    print(f"✅ Exported {len(routes_list)} routes to {output_path}")

conn.close()
