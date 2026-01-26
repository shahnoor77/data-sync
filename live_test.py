#!/usr/bin/env python3
"""
EMQX Cloud Serverless Stress Testing Tool
Tests TLS connectivity, authentication, and high-throughput message delivery
"""

import time
import json
import uuid
import multiprocessing
import os
import sys
import random
import ssl
import socket
from datetime import datetime, timezone
import paho.mqtt.client as mqtt
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Add src to path for CryptoManager
sys.path.append(os.path.join(os.getcwd(), 'src'))
try:
    from sensor_sync.utils.crypto import CryptoManager
except ImportError:
    sys.path.append(os.getcwd())
    from sensor_sync.utils.crypto import CryptoManager

# =========================
# CONFIG
# =========================
BROKER = os.getenv("MQTT_BROKER_HOST")
PORT = int(os.getenv("MQTT_BROKER_PORT", "8883"))
USERNAME = os.getenv("MQTT_USERNAME")
PASSWORD = os.getenv("MQTT_PASSWORD")

# Fix CA cert path - use local file for testing
CA_CERT_PATH = os.getenv("CA_CERT_PATH", "emqx-ca-cert.pem")
if not os.path.exists(CA_CERT_PATH):
    CA_CERT_PATH = "emqx-ca-cert.pem"  # Fallback to local file

TOTAL_MESSAGES = int(os.getenv("TOTAL_MESSAGES", "10000"))
CONCURRENCY = int(os.getenv("CONCURRENCY", "2"))
TOPIC = os.getenv("MQTT_TOPIC_PREFIX", "sensors/live/stress_test")

ENCRYPTION_KEY = os.getenv("ENCRYPTION_KEY")
SIGNING_KEY = os.getenv("SIGNING_KEY")

INFLIGHT_LIMIT = 200
BURST_SIZE = 100
CONNECT_TIMEOUT = 20

# =========================
# VALIDATION
# =========================
def validate():
    print("🔧 Validating configuration...")
    missing = []
    
    if not BROKER: missing.append("MQTT_BROKER_HOST")
    if not USERNAME: missing.append("MQTT_USERNAME") 
    if not PASSWORD: missing.append("MQTT_PASSWORD")
    if not ENCRYPTION_KEY: missing.append("ENCRYPTION_KEY")
    if not SIGNING_KEY: missing.append("SIGNING_KEY")
    
    if missing:
        print(f"❌ Missing required environment variables: {', '.join(missing)}")
        print("❌ Configuration validation failed")
        return False
    
    if not os.path.exists(CA_CERT_PATH):
        print(f"❌ CA certificate not found at: {CA_CERT_PATH}")
        return False

    print("✅ Configuration validation passed")
    print(f"✅ Broker: {BROKER}:{PORT}")
    print(f"✅ Topic: {TOPIC}")
    print(f"✅ CA Cert: {CA_CERT_PATH}")
    return True

# =========================
# CONNECTIVITY TEST
# =========================
def connectivity_test():
    try:
        context = ssl.create_default_context()
        context.load_verify_locations(CA_CERT_PATH)

        with socket.create_connection((BROKER, PORT), timeout=10) as sock:
            with context.wrap_socket(sock, server_hostname=BROKER):
                print("✅ TLS handshake successful")
                return True
    except Exception as e:
        print(f"❌ TLS failed: {e}")
        return False

# =========================
# PRODUCER
# =========================
def producer(proc_id, count):
    sent = acked = 0
    inflight = 0
    connected = False

    crypto = CryptoManager(ENCRYPTION_KEY, SIGNING_KEY)

    def on_connect(c, u, f, rc, p=None):
        nonlocal connected
        connected = (rc == 0)
        print(f"[P{proc_id}] Connected (rc={rc})")

    def on_publish(c, u, mid, rc=None, p=None):
        nonlocal inflight, acked
        inflight -= 1
        acked += 1

    client = mqtt.Client(
        client_id=f"cloud-producer-{proc_id}-{uuid.uuid4().hex[:6]}",
        protocol=mqtt.MQTTv311,
        clean_session=True
    )

    client.username_pw_set(USERNAME, PASSWORD)
    client.tls_set(CA_CERT_PATH, tls_version=ssl.PROTOCOL_TLSv1_2)
    client.max_inflight_messages_set(INFLIGHT_LIMIT)

    client.on_connect = on_connect
    client.on_publish = on_publish

    client.connect_async(BROKER, PORT, keepalive=60)
    client.loop_start()

    t0 = time.time()
    while not connected and time.time() - t0 < CONNECT_TIMEOUT:
        time.sleep(0.1)

    if not connected:
        print(f"[P{proc_id}] ❌ Connect timeout")
        return sent, acked

    for i in range(count):
        while inflight >= INFLIGHT_LIMIT:
            time.sleep(0.01)

        event = {
            "event_id": str(uuid.uuid4()),
            "ts": datetime.now(timezone.utc).isoformat(),
            "sensor_id": f"s-{proc_id}",
            "value": round(random.uniform(20, 30), 2)
        }

        payload = crypto.create_signed_message(event)

        res = client.publish(TOPIC, json.dumps(payload), qos=1)
        if res.rc == mqtt.MQTT_ERR_SUCCESS:
            sent += 1
            inflight += 1

        if sent % 1000 == 0:
            print(f"[P{proc_id}] Sent {sent}")

    while inflight > 0:
        time.sleep(0.1)

    client.disconnect()
    client.loop_stop()

    print(f"[P{proc_id}] Done | Sent={sent} Acked={acked}")
    return sent, acked

# =========================
# MAIN
# =========================
def main():
    print("🚀 EMQX Cloud Stress Test")

    if not validate():
        return 1

    if not connectivity_test():
        return 1

    per_proc = TOTAL_MESSAGES // CONCURRENCY
    args = [(i, per_proc) for i in range(CONCURRENCY)]

    start = time.time()
    with multiprocessing.Pool(CONCURRENCY) as p:
        results = p.starmap(producer, args)

    sent = sum(r[0] for r in results)
    acked = sum(r[1] for r in results)
    dur = time.time() - start

    print("=" * 50)
    print(f"Sent: {sent}")
    print(f"Acked: {acked}")
    print(f"ACK Rate: {(acked/sent)*100:.2f}%")
    print(f"Throughput: {sent/dur:.0f} msg/s")

    return 0 if acked >= sent * 0.95 else 1

if __name__ == "__main__":
    multiprocessing.set_start_method("spawn", force=True)
    exit(main())
