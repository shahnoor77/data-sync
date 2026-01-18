import time
import json
import uuid
import multiprocessing
import paho.mqtt.client as mqtt
from datetime import datetime

# --- CONFIGURATION ---
BROKER = "emqx"
PORT = 1883
TOTAL_MESSAGES = 1000000  # 1 Million messages
PROCESS_COUNT = 4         # Adjust based on your CPU cores
TOPIC = "sensors/industrial/data"

def run_load_worker(worker_id, count):
    """Worker process to hammer the broker"""
    client = mqtt.Client(
        client_id=f"generator_{worker_id}_{uuid.uuid4().hex[:4]}",
        protocol=mqtt.MQTTv311  # Matching your stable subscriber
    )
    
    # High-throughput tuning
    client.max_inflight_messages_set(2000)
    
    try:
        client.connect(BROKER, PORT, keepalive=60)
        client.loop_start()
        
        for i in range(count):
            payload = {
                "s_id": f"sensor_{worker_id}",
                "val": 25.5 + (i % 100),
                "ts": datetime.utcnow().isoformat(),
                "seq": i,
                "sig": "valid_signature_placeholder" # To pass your crypto check
            }
            
            # QoS 0 is used for maximum "firehose" testing
            client.publish(TOPIC, json.dumps(payload), qos=0)
            
            if i % 10000 == 0:
                print(f"Worker {worker_id}: Sent {i}/{count}")

        client.loop_stop()
        client.disconnect()
    except Exception as e:
        print(f"Worker {worker_id} Error: {e}")

if __name__ == "__main__":
    msgs_per_worker = TOTAL_MESSAGES // PROCESS_COUNT
    
    print(f"🚀 Starting Stress Test: {TOTAL_MESSAGES} messages via {PROCESS_COUNT} processes")
    start_time = time.time()
    
    processes = []
    for i in range(PROCESS_COUNT):
        p = multiprocessing.Process(target=run_load_worker, args=(i, msgs_per_worker))
        p.start()
        processes.append(p)
    
    for p in processes:
        p.join()
        
    end_time = time.time()
    duration = end_time - start_time
    print("\n--- TEST COMPLETE ---")
    print(f"Total Time: {duration:.2f} seconds")
    print(f"Avg Throughput: {TOTAL_MESSAGES / duration:.2f} records/sec")