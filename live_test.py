import json
import time
import uuid
from paho.mqtt import client as mqtt
from paho.mqtt.enums import CallbackAPIVersion

# --- CONFIG ---
BROKER = "emqx"
TOPIC = "sensors/data/bulk/sensor_readings"
RECORDS_PER_SECOND = 2000  # A realistic high-load live stream
BATCH_SIZE = 200           # Smaller batches for real-time feel

client = mqtt.Client(callback_api_version=CallbackAPIVersion.VERSION2, protocol=mqtt.MQTTv5)

def run_live_stream():
    client.connect(BROKER, 1883, keepalive=120)
    client.loop_start()
    print(f"📡 Live Stream Simulator Active: {RECORDS_PER_SECOND} msg/sec")
    
    try:
        while True:
            batch = []
            for _ in range(BATCH_SIZE):
                batch.append({
                    "event_id": str(uuid.uuid4()),
                    "table": "sensor_readings",
                    "data": {"val": 25.5, "ts": time.time()},
                    "type": "LIVE_STREAM"
                })
            
            client.publish(TOPIC, json.dumps({"message": batch}), qos=1)
            
            # This maintains the steady flow rate
            time.sleep(BATCH_SIZE / RECORDS_PER_SECOND) 
            
    except KeyboardInterrupt:
        client.loop_stop()

if __name__ == "__main__":
    run_live_stream()