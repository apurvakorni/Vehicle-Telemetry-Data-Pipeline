import json
from dateutil import parser as dtparse
from confluent_kafka import Consumer
import psycopg2
import psycopg2.extras as extras

KAFKA_BOOTSTRAP = "localhost:9092"
KAFKA_TOPIC = "telemetry"  # or your topic name

PG_CONN = dict(
    dbname="telemetry",
    user="postgres",
    password="postgres",
    host="localhost",
    port=5432,
)

# batch insert for speed
BATCH_SIZE = 500

INSERT_SQL = """
INSERT INTO telemetry_raw (
  vehicle_id, ts, odometer_reading, engine_temp_c, engine_rpm, oil_pressure_psi,
  coolant_temp_c, fuel_level_percent, fuel_consumption_lph, engine_load_percent,
  throttle_pos_percent, air_flow_rate_gps, exhaust_gas_temp_c, vibration_level,
  engine_hours, vehicle_speed_kph, brake_fluid_level_psi, brake_pad_wear_mm,
  brake_temp_c, abs_fault_indicator, brake_pedal_pos_percent, wheel_speed_fl_kph,
  wheel_speed_fr_kph, wheel_speed_rl_kph, wheel_speed_rr_kph, battery_voltage_v,
  battery_current_a, battery_temp_c, alternator_output_v, battery_charge_percent,
  battery_health_percent, ambient_temp_c, humidity_percent, gps_latitude,
  gps_longitude, failure_type, failure_date
) VALUES %s
"""

def norm_bool(val):
    if isinstance(val, bool):
        return val
    if val is None:
        return None
    s = str(val).strip().lower()
    return s in ("1", "true", "t", "yes", "y")

def parse_ts(s):
    if not s:
        return None
    return dtparse.parse(s)  # handles ISO & many formats

def row_tuple(d):
    # Map safely; missing keys become None
    g = d.get
    return (
        g("vehicle_id"),
        parse_ts(g("timestamp") or g("ts")),
        to_float(g("odometer_reading")),
        to_float(g("engine_temp_c")),
        to_float(g("engine_rpm")),
        to_float(g("oil_pressure_psi")),
        to_float(g("coolant_temp_c")),
        to_float(g("fuel_level_percent")),
        to_float(g("fuel_consumption_lph")),
        to_float(g("engine_load_percent")),
        to_float(g("throttle_pos_percent")),
        to_float(g("air_flow_rate_gps")),
        to_float(g("exhaust_gas_temp_c")),
        to_float(g("vibration_level")),
        to_float(g("engine_hours")),
        to_float(g("vehicle_speed_kph")),
        to_float(g("brake_fluid_level_psi")),
        to_float(g("brake_pad_wear_mm")),
        to_float(g("brake_temp_c")),
        norm_bool(g("abs_fault_indicator")),
        to_float(g("brake_pedal_pos_percent")),
        to_float(g("wheel_speed_fl_kph")),
        to_float(g("wheel_speed_fr_kph")),
        to_float(g("wheel_speed_rl_kph")),
        to_float(g("wheel_speed_rr_kph")),
        to_float(g("battery_voltage_v")),
        to_float(g("battery_current_a")),
        to_float(g("battery_temp_c")),
        to_float(g("alternator_output_v")),
        to_float(g("battery_charge_percent")),
        to_float(g("battery_health_percent")),
        to_float(g("ambient_temp_c")),
        to_float(g("humidity_percent")),
        to_float(g("gps_latitude")),
        to_float(g("gps_longitude")),
        g("failure_type"),
        g("failure_date"),
    )

def to_float(x):
    if x is None or x == "":
        return None
    try:
        return float(x)
    except Exception:
        return None

def main():
    consumer = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "group.id": "ingestors",
        "auto.offset.reset": "earliest"
    })
    consumer.subscribe([KAFKA_TOPIC])

    conn = psycopg2.connect(**PG_CONN)
    cur = conn.cursor()

    buffer = []

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                # flush batches periodically when idle
                if buffer:
                    extras.execute_values(cur, INSERT_SQL, buffer, page_size=BATCH_SIZE)
                    conn.commit()
                    buffer.clear()
                continue

            if msg.error():
                print("Kafka error:", msg.error())
                continue

            data = json.loads(msg.value())
            buffer.append(row_tuple(data))

            if len(buffer) >= BATCH_SIZE:
                extras.execute_values(cur, INSERT_SQL, buffer, page_size=BATCH_SIZE)
                conn.commit()
                buffer.clear()

    except KeyboardInterrupt:
        pass
    finally:
        if buffer:
            extras.execute_values(cur, INSERT_SQL, buffer, page_size=BATCH_SIZE)
            conn.commit()
        cur.close()
        conn.close()
        consumer.close()

if __name__ == "__main__":
    main()

