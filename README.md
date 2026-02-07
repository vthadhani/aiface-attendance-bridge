# Aiface Attendance Bridge (MQTT -> REST)

This service subscribes to Aiface MQTT attendance logs and exposes a REST API your POS can pull from.

## REST API
All endpoints (except /health) require:
Authorization: Bearer <API_TOKEN>

### Health
GET /health

### Latest logs
GET /logs/latest?limit=50

### Logs with optional time filter
GET /logs?since=2026-02-07T00:00:00.000Z&limit=200&offset=0

### Logs by employee enrollid
GET /logs/employee/:enrollid?limit=200&offset=0

## Persistence
Mount a persistent directory to /app/data.
