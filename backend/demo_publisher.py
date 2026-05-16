#!/usr/bin/env python3
"""
backend/demo_publisher.py
==========================
Publishes a set of demo hazards to MQTT so you can see the
map populate immediately without needing real users.

Run:  python backend/demo_publisher.py
"""

import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import json, uuid, time, random
import paho.mqtt.client as mqtt
from paho.mqtt.enums import CallbackAPIVersion
from config.settings import MQTT_BROKER, MQTT_PORT, TOPIC_REPORT, TOPIC_LOCATION

# ── Sample data (around Manila, Philippines — change to your city) ────────────
BASE_LAT, BASE_LON = 14.5995, 120.9842   # ← change to your city centre

DEMO_HAZARDS = [
    {"category": "Pothole",              "severity": "critical",  "description": "Large pothole covering half the lane, 30cm deep"},
    {"category": "Flood / Standing Water","severity": "high",     "description": "Standing water after heavy rain, ankle deep"},
    {"category": "Road Accident",        "severity": "critical",  "description": "Two-vehicle collision blocking pedestrian path"},
    {"category": "Debris / Fallen Object","severity": "medium",   "description": "Tree branch fallen across walkway"},
    {"category": "Construction Zone",    "severity": "medium",    "description": "Unmarked construction site, missing barriers"},
    {"category": "Broken Pavement",      "severity": "low",       "description": "Cracked tiles, slight trip hazard"},
    {"category": "Poor Lighting",        "severity": "low",       "description": "Streetlamp out, dark section at night"},
    {"category": "Aggressive Animal",    "severity": "high",      "description": "Stray dog reported aggressive near market"},
]

DEMO_USERS = ["user_alice", "user_bob", "user_carlos", "user_diana"]


def rand_coord(base_lat, base_lon, radius_deg=0.02):
    return (
        base_lat + random.uniform(-radius_deg, radius_deg),
        base_lon + random.uniform(-radius_deg, radius_deg),
    )


def main():
    client = mqtt.Client(
        callback_api_version=CallbackAPIVersion.VERSION1,
        client_id=f"demo_{uuid.uuid4().hex[:6]}",
    )

    connected = False
    def on_connect(c, ud, flags, rc):
        nonlocal connected
        connected = (rc == 0)
        if connected:
            print(f"✔ Connected to {MQTT_BROKER}")
        else:
            print(f"✘ Failed rc={rc}")

    client.on_connect = on_connect
    client.connect(MQTT_BROKER, MQTT_PORT, 60)
    client.loop_start()

    for _ in range(20):
        if connected:
            break
        time.sleep(0.5)

    if not connected:
        print("Could not connect. Is the internet available?")
        return

    print(f"\nPublishing {len(DEMO_HAZARDS)} demo hazards…\n")
    for h in DEMO_HAZARDS:
        lat, lon = rand_coord(BASE_LAT, BASE_LON)
        user = random.choice(DEMO_USERS)
        payload = {
            "report_id":   str(uuid.uuid4()),
            "user_id":     user,
            "lat":         round(lat, 6),
            "lon":         round(lon, 6),
            "category":    h["category"],
            "severity":    h["severity"],
            "description": h["description"],
            "timestamp":   time.time(),
            "votes":       random.randint(0, 12),
            "image_b64":   None,
        }
        client.publish(TOPIC_REPORT, json.dumps(payload))
        print(f"  [{h['severity'].upper():8}] {h['category']:28} @ ({lat:.4f},{lon:.4f})")
        time.sleep(0.4)

    print("\nStreaming demo user locations for 20 seconds…")
    for i in range(10):
        for uid in DEMO_USERS:
            lat, lon = rand_coord(BASE_LAT, BASE_LON, 0.005)
            loc_payload = {
                "user_id":   uid,
                "lat":       round(lat, 6),
                "lon":       round(lon, 6),
                "timestamp": time.time(),
            }
            client.publish(TOPIC_LOCATION, json.dumps(loc_payload))
        time.sleep(2)
        print(f"  Location update {i+1}/10")

    print("\n✔ Demo done. Open http://localhost:5000 to see the map.")
    client.loop_stop()
    client.disconnect()


if __name__ == "__main__":
    main()
