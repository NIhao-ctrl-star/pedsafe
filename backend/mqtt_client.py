#!/usr/bin/env python3
"""
backend/mqtt_client.py
========================
Command-line tool for:
  • Submitting hazard reports via MQTT  (publish to TOPIC_REPORT)
  • Streaming a user's GPS position      (publish to TOPIC_LOCATION)
  • Listening to live alerts              (subscribe to TOPIC_ALERT)

Run:  python backend/mqtt_client.py
"""

import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import json, uuid, time, threading, base64
from datetime import datetime

import paho.mqtt.client as mqtt
from paho.mqtt.enums import CallbackAPIVersion

from config.settings import (
    MQTT_BROKER, MQTT_PORT, MQTT_KEEPALIVE,
    TOPIC_REPORT, TOPIC_LOCATION, TOPIC_ALERT, TOPIC_ACK,
    HAZARD_CATEGORIES,
)

USER_ID = f"user_{uuid.uuid4().hex[:8]}"
CLIENT_ID = f"ped_client_{uuid.uuid4().hex[:6]}"

# ── ANSI colours ──────────────────────────────────────────────────────────────
RED    = "\033[91m"
YEL    = "\033[93m"
GRN    = "\033[92m"
BLU    = "\033[94m"
CYAN   = "\033[96m"
RESET  = "\033[0m"
BOLD   = "\033[1m"

def print_banner():
    print(f"""
{CYAN}{BOLD}╔══════════════════════════════════════════════════════════╗
║   🚶 Pedestrian Safety — MQTT Reporter Client            ║
║   User ID : {USER_ID:<42} ║
║   Broker  : {MQTT_BROKER:<42} ║
╚══════════════════════════════════════════════════════════╝{RESET}
""")

# ── MQTT callbacks ────────────────────────────────────────────────────────────
def on_connect(client, userdata, flags, reason_code, properties):
    rc = reason_code
    if rc == 0:
        print(f"{GRN}✔ Connected to {MQTT_BROKER}:{MQTT_PORT}{RESET}")
        client.subscribe([(TOPIC_ALERT, 0), (TOPIC_ACK, 0)])
    else:
        print(f"{RED}✘ Connection failed (rc={rc}){RESET}")

def on_message(client, userdata, msg):
    try:
        data = json.loads(msg.payload.decode())
    except Exception:
        return

    if msg.topic == TOPIC_ACK:
        print(f"\n{GRN}✔ Report acknowledged: {data.get('report_id','?')}{RESET}\n> ", end="", flush=True)

    elif msg.topic == TOPIC_ALERT:
        print(f"\n{RED}{BOLD}🚨 ALERT — Hazard nearby!{RESET}")
        for a in data.get("alerts", []):
            print(f"  {RED}▶ {a['category']} ({a['severity'].upper()}) — {a['distance_m']} m away{RESET}")
            print(f"    📍 ({a['lat']}, {a['lon']})  | {a['description']}")
        print("> ", end="", flush=True)

def on_disconnect(client, userdata, flags, reason_code, properties=None):
    rc = reason_code
    print(f"{YEL}⚠ Disconnected (rc={rc}){RESET}")


# ── Report builder ────────────────────────────────────────────────────────────
def build_report(lat: float, lon: float) -> dict:
    print(f"\n{BOLD}--- New Hazard Report ---{RESET}")

    # Category
    print("Category:")
    for i, c in enumerate(HAZARD_CATEGORIES, 1):
        print(f"  {i}. {c}")
    while True:
        try:
            choice = int(input("  Choose (1-{}): ".format(len(HAZARD_CATEGORIES))))
            category = HAZARD_CATEGORIES[choice - 1]
            break
        except (ValueError, IndexError):
            print("  Invalid choice, try again.")

    # Severity
    print("Severity: 1=low  2=medium  3=high  4=critical")
    sev_map = {"1": "low", "2": "medium", "3": "high", "4": "critical"}
    while True:
        s = input("  Choose (1-4): ").strip()
        if s in sev_map:
            severity = sev_map[s]
            break
        print("  Invalid, try again.")

    description = input("Short description: ").strip() or "No description."

    # Optional image
    image_b64 = None
    img_path = input("Image path (leave blank to skip): ").strip()
    if img_path and os.path.exists(img_path):
        with open(img_path, "rb") as f:
            image_b64 = base64.b64encode(f.read()).decode()
        print(f"  {GRN}Image attached ({os.path.getsize(img_path)} bytes){RESET}")

    return {
        "report_id":   str(uuid.uuid4()),
        "user_id":     USER_ID,
        "lat":         lat,
        "lon":         lon,
        "category":    category,
        "severity":    severity,
        "description": description,
        "timestamp":   time.time(),
        "votes":       0,
        "image_b64":   image_b64,
    }


# ── Location streaming ────────────────────────────────────────────────────────
_streaming = False
_stream_thread = None

def start_location_stream(client, lat: float, lon: float, interval: float = 5.0):
    global _streaming, _stream_thread
    _streaming = True

    def _loop():
        cur_lat, cur_lon = lat, lon
        print(f"{CYAN}Streaming location every {interval}s — type 'stoploc' to stop{RESET}")
        while _streaming:
            payload = json.dumps({
                "user_id":   USER_ID,
                "lat":       round(cur_lat, 6),
                "lon":       round(cur_lon, 6),
                "timestamp": time.time(),
            })
            client.publish(TOPIC_LOCATION, payload)
            time.sleep(interval)

    _stream_thread = threading.Thread(target=_loop, daemon=True)
    _stream_thread.start()


def stop_location_stream():
    global _streaming
    _streaming = False
    print(f"{YEL}Location stream stopped.{RESET}")


# ── Menu ──────────────────────────────────────────────────────────────────────
def print_menu():
    print(f"""
{BOLD}Commands:{RESET}
  {YEL}r{RESET}  — Submit a hazard report
  {YEL}l{RESET}  — Start streaming my location
  {YEL}s{RESET}  — Stop location stream
  {YEL}q{RESET}  — Quit
""")


def get_coords(prompt: str = "Enter lat,lon (e.g. 14.5995,120.9842): ") -> tuple:
    while True:
        raw = input(prompt).strip()
        try:
            lat_s, lon_s = raw.split(",")
            return float(lat_s.strip()), float(lon_s.strip())
        except ValueError:
            print("  Format must be: <latitude>,<longitude>")


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    print_banner()

    client = mqtt.Client(
        callback_api_version=CallbackAPIVersion.VERSION1,
        client_id=CLIENT_ID,
    )
    client.on_connect    = on_connect
    client.on_message    = on_message
    client.on_disconnect = on_disconnect

    print(f"Connecting to {MQTT_BROKER}:{MQTT_PORT} …")
    try:
        client.connect(MQTT_BROKER, MQTT_PORT, MQTT_KEEPALIVE)
    except Exception as e:
        print(f"{RED}Connection error: {e}{RESET}")
        sys.exit(1)

    client.loop_start()
    time.sleep(1.5)   # wait for on_connect

    print_menu()
    try:
        while True:
            cmd = input("> ").strip().lower()

            if cmd == "r":
                lat, lon = get_coords()
                report = build_report(lat, lon)
                payload = json.dumps(report)
                client.publish(TOPIC_REPORT, payload)
                print(f"{GRN}Report sent! (id={report['report_id']}){RESET}")

            elif cmd == "l":
                lat, lon = get_coords("Enter YOUR current lat,lon: ")
                interval_s = input("Update interval in seconds [5]: ").strip()
                interval = float(interval_s) if interval_s else 5.0
                start_location_stream(client, lat, lon, interval)

            elif cmd == "s" or cmd == "stoploc":
                stop_location_stream()

            elif cmd in ("q", "quit", "exit"):
                stop_location_stream()
                print("Goodbye!")
                break

            elif cmd == "?":
                print_menu()

            elif cmd:
                print(f"Unknown command '{cmd}'. Type '?' for help.")

    except KeyboardInterrupt:
        pass
    finally:
        stop_location_stream()
        client.loop_stop()
        client.disconnect()


if __name__ == "__main__":
    main()
