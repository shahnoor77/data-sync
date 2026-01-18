#!/bin/bash
set -e


echo "Starting Sensor Sync Service"
echo "Service Mode: ${SERVICE_MODE:-publisher}"


# Wait for dependencies
if [ "$SERVICE_MODE" = "publisher" ]; then
    echo "Waiting for source database..."
    until pg_isready -h "${SOURCE_DB_HOST}" -p "${SOURCE_DB_PORT}" -U "${SOURCE_DB_USER}" 2>/dev/null; do
        echo "Source database not ready, waiting..."
        sleep 2
    done
    echo "✓ Source database ready"
    
    echo "Waiting for MQTT broker..."
    until python -c "import socket; s=socket.socket(); s.connect(('${MQTT_BROKER_HOST}', ${MQTT_BROKER_PORT})); s.close()" 2>/dev/null; do
        echo "MQTT broker not ready, waiting..."
        sleep 2
    done
    echo "✓ MQTT broker ready"
fi

if [ "$SERVICE_MODE" = "subscriber" ]; then
    echo "Waiting for target database..."
    if [ "$TARGET_DB_TYPE" = "mongodb" ]; then
        until python -c "from pymongo import MongoClient; MongoClient('mongodb://${TARGET_DB_USER}:${TARGET_DB_PASSWORD}@${TARGET_DB_HOST}:${TARGET_DB_PORT}/').admin.command('ping')" 2>/dev/null; do
            echo "Target database not ready, waiting..."
            sleep 2
        done
    elif [ "$TARGET_DB_TYPE" = "postgresql" ]; then
        until pg_isready -h "${TARGET_DB_HOST}" -p "${TARGET_DB_PORT}" -U "${TARGET_DB_USER}" 2>/dev/null; do
            echo "Target database not ready, waiting..."
            sleep 2
        done
    fi
    echo "✓ Target database ready"
    
    echo "Waiting for MQTT broker..."
    until python -c "import socket; s=socket.socket(); s.connect(('${MQTT_BROKER_HOST}', ${MQTT_BROKER_PORT})); s.close()" 2>/dev/null; do
        echo "MQTT broker not ready, waiting..."
        sleep 2
    done
    echo "✓ MQTT broker ready"
fi


echo "All dependencies ready, starting service..."


# Run the application
exec python -m src.sensor_sync.main