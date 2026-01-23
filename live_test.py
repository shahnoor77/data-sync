#!/usr/bin/env python3
"""
High-Throughput Multi-Processed Stress Testing Tool (Realistic Sensor Simulation)
"""

import time
import json
import uuid
import multiprocessing
import os
import sys
import random
from datetime import datetime, timezone
import paho.mqtt.client as mqtt

# Add src to path for CryptoManager
sys.path.append(os.path.join(os.getcwd(), 'src'))
try:
    from sensor_sync.utils.crypto import CryptoManager
except ImportError:
    sys.path.append(os.getcwd())
    from sensor_sync.utils.crypto import CryptoManager

# =========================
# Configuration
# =========================
BROKER = os.getenv('MQTT_BROKER_HOST')
PORT = int(os.getenv('MQTT_BROKER_PORT'))
USERNAME = os.getenv('MQTT_USERNAME')
PASSWORD = os.getenv('MQTT_PASSWORD')
CA_CERT_PATH = os.getenv('CA_CERT_PATH')
TOTAL_MESSAGES = int(os.getenv('TOTAL_MESSAGES', '1000000'))
CONCURRENCY = int(os.getenv('CONCURRENCY', '4'))
TARGET_TOPIC = os.getenv('TARGET_TOPIC', 'sensors/live/stress_test')

# Burst Mode Configuration
BURST_SIZE = 5000
INFLIGHT_THRESHOLD = 1000
FLOW_CONTROL_SLEEP = 0.001  # 1ms

# Crypto keys
ENCRYPTION_KEY = os.getenv('ENCRYPTION_KEY')
SIGNING_KEY = os.getenv('SIGNING_KEY')


# =========================
# Producer Process
# =========================
def run_producer_process(process_id: int, message_count: int):
    stats = {'messages_sent': 0, 'messages_acked': 0, 'crypto_failures': 0, 'flow_control_pauses': 0}
    sequence_number = 0
    inflight_count = 0

    def on_publish(client, userdata, mid, reason_code, properties=None):
        nonlocal inflight_count
        inflight_count -= 1
        stats['messages_acked'] += 1

    # MQTT client setup with TLS
    client_id = f"cloud_stress_producer_{process_id}_{uuid.uuid4().hex[:6]}"
    client = mqtt.Client(
        callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
        client_id=client_id,
        protocol=mqtt.MQTTv311,
        clean_session=False
    )
    
    # TLS Configuration for EMQX Cloud
    if os.path.exists(CA_CERT_PATH):
        client.tls_set(ca_certs=CA_CERT_PATH)
        print(f"Producer {process_id}: TLS enabled with CA certificate")
    else:
        print(f"Producer {process_id}: Warning - CA certificate not found at {CA_CERT_PATH}")
    
    # Authentication
    if USERNAME and PASSWORD:
        client.username_pw_set(USERNAME, PASSWORD)
        print(f"Producer {process_id}: Authentication configured")
    
    client.max_inflight_messages_set(2000)
    client.on_publish = on_publish

    # Crypto Manager
    crypto = CryptoManager(ENCRYPTION_KEY, SIGNING_KEY)

    print(f"Producer {process_id}: Connecting to {BROKER}:{PORT}")
    client.connect_async(BROKER, PORT, keepalive=60)
    client.loop_start()

    # Wait for connection
    start_time = time.time()
    while not client.is_connected() and (time.time() - start_time) < 10:
        time.sleep(0.1)

    if not client.is_connected():
        print(f"Producer {process_id}: Connection failed")
        return

    print(f"Producer {process_id}: Connected, sending {message_count} messages")
    total_sent = 0
    test_start = time.time()

    while total_sent < message_count:
        # Adaptive burst
        burst_size = min(BURST_SIZE, INFLIGHT_THRESHOLD - inflight_count, message_count - total_sent)
        if burst_size <= 0:
            # Wait for inflight to reduce
            while inflight_count > INFLIGHT_THRESHOLD * 0.7:
                stats['flow_control_pauses'] += 1
                time.sleep(FLOW_CONTROL_SLEEP)
            continue

        for _ in range(burst_size):
            sequence_number += 1
            trace_id = str(uuid.uuid4())
            timestamp = datetime.now(timezone.utc).isoformat()
            envelope_version = "1.0"

            # Realistic sensor data
            event = {
                "trace_id": trace_id,
                "sequence_number": sequence_number,
                "process_id": process_id,
                "timestamp": timestamp,
                "envelope_version": envelope_version,
                "event_id": f"stress_{process_id}_{sequence_number}_{uuid.uuid4().hex[:8]}",
                "table": "sensor_readings",
                "operation": "INSERT",
                "sent_at": time.time(),
                "data": {
                    "sensor_id": f"sensor_{process_id}",
                    "temperature": round(random.uniform(15.0, 30.0), 2),
                    "humidity": round(random.uniform(30.0, 80.0), 2),
                    "pressure": round(random.uniform(950.0, 1050.0), 2),
                    "location": f"zone_{process_id}"
                },
                "envelope_metadata": {
                    "version": envelope_version,
                    "schema_version": "1.0",
                    "format": "json",
                    "created_by": "stress_test",
                    "created_at": timestamp
                }
            }

            # Sign & encrypt
            try:
                signed_message = crypto.create_signed_message(event)
                payload = {"message": signed_message}
            except Exception:
                stats['crypto_failures'] += 1
                continue

            # Publish
            result = client.publish(TARGET_TOPIC, json.dumps(payload, separators=(',', ':')), qos=2)
            if result.rc == mqtt.MQTT_ERR_SUCCESS:
                inflight_count += 1
                stats['messages_sent'] += 1
                total_sent += 1

        # Flow control
        if inflight_count > INFLIGHT_THRESHOLD * 0.9:
            while inflight_count > INFLIGHT_THRESHOLD * 0.7:
                stats['flow_control_pauses'] += 1
                time.sleep(FLOW_CONTROL_SLEEP)

        # Progress update
        if total_sent % 50000 == 0:
            elapsed = time.time() - test_start
            rate = total_sent / elapsed if elapsed > 0 else 0
            print(f"Producer {process_id}: {total_sent}/{message_count} ({rate:.0f} msg/s)")

    # Wait for all acks
    while inflight_count > 0:
        time.sleep(0.01)

    client.loop_stop()
    client.disconnect()
    duration = time.time() - test_start
    final_rate = message_count / duration if duration > 0 else 0
    print(f"Producer {process_id}: Complete - {stats['messages_sent']} sent, "
          f"{stats['messages_acked']} acked, {final_rate:.0f} msg/s")


# =========================
# Main Runner
# =========================
def main():
    print(f"🚀 High-Throughput Stress Test")
    print(f"Messages: {TOTAL_MESSAGES:,}, Processes: {CONCURRENCY}")
    print(f"Topic: {TARGET_TOPIC}, Broker: {BROKER}:{PORT}")

    messages_per_process = TOTAL_MESSAGES // CONCURRENCY
    remainder = TOTAL_MESSAGES % CONCURRENCY

    # Prepare process arguments
    process_args = []
    for i in range(CONCURRENCY):
        count = messages_per_process + (1 if i < remainder else 0)
        process_args.append((i, count))

    start_time = time.time()

    # Start multi-process pool
    with multiprocessing.Pool(processes=CONCURRENCY) as pool:
        pool.starmap(run_producer_process, process_args)

    duration = time.time() - start_time
    throughput = TOTAL_MESSAGES / duration if duration > 0 else 0

    print(f"\n✅ Test Complete")
    print(f"Duration: {duration:.2f}s")
    print(f"Throughput: {throughput:.0f} msg/s")


if __name__ == "__main__":
    multiprocessing.set_start_method('spawn', force=True)
    main()
