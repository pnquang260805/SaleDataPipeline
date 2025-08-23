#!/bin/bash

superset fab create-admin \
  --username admin \
  --firstname Superset \
  --lastname Admin \
  --email admin@example.com \
  --password adminPassWord

superset db upgrade
superset init

exec superset run -h 0.0.0.0 -p 8088