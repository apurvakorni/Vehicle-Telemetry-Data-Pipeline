# consumer.py
from confluent_kafka import Consumer

c = Consumer({
    "bootstrap.servers": "localhost:9092",
    "group.id": "demo-group",
    "auto.offset.reset": "earliest"
})
c.subscribe(["telemetry"])

try:
    while True:
        msg = c.poll(1.0)
        if msg and not msg.error():
            print(msg.value().decode("utf-8"))
finally:
    c.close()
