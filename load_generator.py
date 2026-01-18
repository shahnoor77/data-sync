import json
import time
import uuid
import multiprocessing
from paho.mqtt import client as mqtt

# --- CONFIGURATION ---
BROKER = "emqx"          # Use the service name from your docker-compose
PORT = 1883
TOTAL_RECORDS = 1000000  # 1 Million
BATCH_SIZE = 5000        # How many records to pack into one MQTT message
TOPIC = "sensors/data/bulk/sensor_readings"
CONCURRENCY = 4          # Parallel processes to saturate the network

def generate_load(process_id, count):
    # MQTT 5.0 for high reliability and performance
    client = mqtt.Client(client_id=f"stress_test_{process_id}_{uuid.uuid4().hex[:4]}", protocol=mqtt.MQTTv5)
    
    try:
        client.connect(BROKER, PORT, keepalive=60)
        client.loop_start()
        
        records_sent = 0
        while records_sent < count:
            batch = []
            for _ in range(min(BATCH_SIZE, count - records_sent)):
                batch.append({
                    "event_id": str(uuid.uuid4()),
                    "table": "sensor_readings",
                    "operation": "INSERT",
                    "data": {
                        "sensor_id": f"SENS_{process_id}",
                        "reading": 25.5 + (process_id * 0.1),
                        "status": "online"
                    }
                })
            
            # Wrap in the format the Publisher expects
            payload = json.dumps({"message": batch})
            
            # Use QoS 1 for high speed; QoS 2 for absolute 99.9% guarantee
            client.publish(TOPIC, payload, qos=1)
            
            records_sent += len(batch)
            if records_sent % 50000 == 0:
                print(f"Process {process_id}: Sent {records_sent}/{count}...")

        client.loop_stop()
        client.disconnect()
    except Exception as e:
        print(f"Error in Process {process_id}: {e}")

if __name__ == "__main__":
    print(f"🚀 Starting Stress Test: {TOTAL_RECORDS} records via {CONCURRENCY} workers")
    start_time = time.time()
    per_process = TOTAL_RECORDS // CONCURRENCY
    
    processes = [multiprocessing.Process(target=generate_load, args=(i, per_process)) for i in range(CONCURRENCY)]
    
    for p in processes: p.start()
    for p in processes: p.join()

    duration = time.time() - start_time
    print(f"\n--- TEST COMPLETE ---")
    print(f"Total Time: {duration:.2f} seconds")
    print(f"Avg Throughput: {TOTAL_RECORDS / duration:.2f} records/sec")