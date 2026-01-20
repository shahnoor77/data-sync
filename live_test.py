#!/usr/bin/env python3
"""
High-Throughput Multi-Processed Stress Testing Tool
"""

import time
import json
import uuid
import multiprocessing
import os
import sys
from datetime import datetime, timezone
import paho.mqtt.client as mqtt

# Add src to path
sys.path.append(os.path.join(os.getcwd(), 'src'))
try:
    from sensor_sync.utils.crypto import CryptoManager
except ImportError:
    sys.path.append(os.getcwd())
    from sensor_sync.utils.crypto import CryptoManager

# Configuration
BROKER = os.getenv('MQTT_BROKER', 'emqx')
PORT = int(os.getenv('MQTT_PORT', '1883'))
TOTAL_MESSAGES = int(os.getenv('TOTAL_MESSAGES', '1000000'))
CONCURRENCY = int(os.getenv('CONCURRENCY', '4'))
TARGET_TOPIC = os.getenv('TARGET_TOPIC', 'sensors/live/stress_test')

# Burst Mode Configuration
BURST_SIZE = 5000
INFLIGHT_THRESHOLD = 1000
FLOW_CONTROL_SLEEP = 0.01  # 10ms

# Crypto keys
ENCRYPTION_KEY = os.getenv('ENCRYPTION_KEY', "7V6rZp9yXw2mQ8nB5jL1kH4sT3vG6aF9dC2eR1oN0uM=")
SIGNING_KEY = os.getenv('SIGNING_KEY', "MySigningKeyAndWeCanAdjustItAsWeWant.")


def run_producer_process(process_id: int, message_count: int):
    """High-throughput producer process"""
    
    # Statistics
    stats = {
        'messages_sent': 0,
        'messages_acked': 0,
        'crypto_failures': 0,
        'flow_control_pauses': 0
    }
    
    sequence_number = 0
    inflight_count = 0
    
    def on_publish(client, userdata, mid, reason_code, properties=None):
        nonlocal inflight_count
        inflight_count -= 1
        stats['messages_acked'] += 1
    
    # Setup MQTT client with Session Wipe capability
    client_id = f"stress_producer_{process_id}_{uuid.uuid4().hex[:6]}"
    client = mqtt.Client(
        callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
        client_id=client_id,
        protocol=mqtt.MQTTv311,
        clean_session=False  # Start with persistent sessions
    )
    
    client.max_inflight_messages_set(2000)
    client.on_publish = on_publish
    
    # Initialize crypto
    crypto = CryptoManager(ENCRYPTION_KEY, SIGNING_KEY)
    
    try:
        # Use connect_async() + loop_start() for better lifecycle management
        client.connect_async(BROKER, PORT, keepalive=60)  # Increase Keep-Alive: 60 seconds
        client.loop_start()
        
        # Wait for connection
        timeout = 10
        start_time = time.time()
        while not client.is_connected() and (time.time() - start_time) < timeout:
            time.sleep(0.1)
        
        if not client.is_connected():
            print(f"Producer {process_id}: Connection failed")
            return
        
        print(f"Producer {process_id}: Connected, sending {message_count} messages")
        
        total_sent = 0
        test_start = time.time()
        
        while total_sent < message_count:
            # Burst mode: send messages as fast as possible
            burst_size = min(BURST_SIZE, message_count - total_sent)
            
            for _ in range(burst_size):
                sequence_number += 1
                
                # Packet Identity with Future-Proofing envelope
                trace_id = str(uuid.uuid4())
                timestamp = datetime.now(timezone.utc).isoformat()
                envelope_version = "1.0"  # Future-Proofing: Version field
                
                event = {
                    "trace_id": trace_id,
                    "sequence_number": sequence_number,
                    "process_id": process_id,
                    "timestamp": timestamp,
                    "envelope_version": envelope_version,  # Future-Proofing
                    "event_id": f"stress_{process_id}_{sequence_number}_{uuid.uuid4().hex[:8]}",
                    "table": "sensor_readings",
                    "operation": "INSERT",
                    "sent_at": time.time_ns() / 1_000_000_000,
                    "data": {
                        "sensor_id": f"stress_sensor_{process_id}",
                        "temperature": 20.0 + (sequence_number % 50),
                        "humidity": 40.0 + (sequence_number % 60),
                        "pressure": 1000.0 + (sequence_number % 100),
                        "location": f"stress_zone_{process_id}",
                        "trace_id": trace_id,
                        "sequence_number": sequence_number,
                        "process_id": process_id
                    },
                    # Future-Proofing: Envelope metadata
                    "envelope_metadata": {
                        "version": envelope_version,
                        "schema_version": "1.0",
                        "format": "json",
                        "created_by": "stress_test",
                        "created_at": timestamp
                    }
                }
                
                # Sign message
                try:
                    signed_message = crypto.create_signed_message(event)
                    payload = {"message": signed_message}
                except Exception as e:
                    stats['crypto_failures'] += 1
                    continue
                
                # Publish
                result = client.publish(TARGET_TOPIC, json.dumps(payload, separators=(',', ':')), qos=2)
                if result.rc == mqtt.MQTT_ERR_SUCCESS:
                    inflight_count += 1
                    stats['messages_sent'] += 1
                    total_sent += 1
            
            # Flow Control
            if inflight_count > INFLIGHT_THRESHOLD:
                time.sleep(FLOW_CONTROL_SLEEP)
                stats['flow_control_pauses'] += 1
            
            # Progress
            if total_sent % 50000 == 0:
                elapsed = time.time() - test_start
                rate = total_sent / elapsed if elapsed > 0 else 0
                print(f"Producer {process_id}: {total_sent}/{message_count} ({rate:.0f} msg/s)")
        
        # Wait for ACKs
        while inflight_count > 0:
            time.sleep(0.1)
        
        client.loop_stop()
        client.disconnect()
        
        duration = time.time() - test_start
        final_rate = message_count / duration if duration > 0 else 0
        
        print(f"Producer {process_id}: Complete - {stats['messages_sent']} sent, "
              f"{stats['messages_acked']} acked, {final_rate:.0f} msg/s")
        
    except Exception as e:
        print(f"Producer {process_id}: Error - {e}")


def main():
    """Main stress test runner"""
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
    
    # Start processes
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