#!/usr/bin/env bash
set -e

echo "▶ Airflow DB migration"
airflow db migrate

echo "▶ Creating admin user (if not exists)"
airflow users create \
  --username admin \
  --password admin \
  --firstname Admin \
  --lastname User \
  --role Admin \
  --email admin@example.com || true

echo "▶ Airflow init completed"