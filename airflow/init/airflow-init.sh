#!/usr/bin/env bash


airflow db migrate

airflow users create --username admin --password admin --firstname Admin --lastname User --role Admin --email admin@example.com