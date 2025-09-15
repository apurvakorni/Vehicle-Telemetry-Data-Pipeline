from confluent_kafka import Producer
import json, time, random

p = Producer({"bootstrap.servers": "localhost:9092"})

for i in range(50):   # send exactly 50 messages
    event = {
        "vehicle_id": f"V_{random.randint(1,50)}",
        "speed_kph": random.randint(0, 140),
        "battery_pct": round(random.uniform(5, 100), 1),
        "engine_temp_c": round(random.uniform(70, 120), 1)
    }
    p.produce("telemetry", json.dumps(event).encode("utf-8"), key=event["vehicle_id"])
    p.poll(0)        # let Kafka handle delivery
    time.sleep(0.2)  # ~5 messages per second

# make sure all 50 are delivered before exit
p.flush()
print("✅ Sent 50 messages and stopped cleanly.")
