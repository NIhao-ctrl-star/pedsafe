#!/usr/bin/env python3
"""
pedestrian_safety/backend/server.py
====================================
Flask + Flask-SocketIO server that bridges MQTT ↔ WebSocket.

All hazard reports received from MQTT are:
  1. Stored in memory (+ optional flat JSON file for persistence)
  2. Broadcast to every connected browser via SocketIO
  3. Checked against connected users' positions → alert if too close

NEW in this version:
  • resolve_hazard  SocketIO event  — removes a hazard and broadcasts removal
  • /api/hazards.geojson            — GeoJSON endpoint for QGIS / other GIS tools
  • /api/hazard/<id>  DELETE        — REST endpoint to delete a hazard
  • /api/hazard/<id>/resolve  POST  — REST alias for resolve
"""

import sys, os
_HERE = os.path.dirname(os.path.abspath(__file__))
_ROOT = os.path.dirname(_HERE) if os.path.basename(_HERE) == "backend" else _HERE
sys.path.insert(0, _ROOT)

import json, uuid, time, threading, logging
from datetime import datetime

import paho.mqtt.client as mqtt
from paho.mqtt.enums import CallbackAPIVersion
from flask import Flask, render_template, request, jsonify
from flask_socketio import SocketIO, emit
from flask_cors import CORS
from haversine import haversine, Unit

from config.settings import (
    MQTT_BROKER, MQTT_PORT, MQTT_KEEPALIVE,
    TOPIC_REPORT, TOPIC_LOCATION, TOPIC_ALERT, TOPIC_ACK,
    ALERT_RADIUS_METERS, SEVERITY_COLORS,
    FLASK_HOST, FLASK_PORT, SECRET_KEY, REPORT_EXPIRY_SECONDS,
)

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("PedSafety")

# ── Flask app ─────────────────────────────────────────────────────────────────
app = Flask(
    __name__,
    template_folder=os.path.join(os.path.dirname(__file__), "..", "frontend"),
    static_folder=os.path.join(os.path.dirname(__file__), "..", "frontend", "static"),
)
app.config["SECRET_KEY"] = SECRET_KEY
CORS(app)
socketio = SocketIO(app, cors_allowed_origins="*", async_mode="eventlet")

# ── In-memory stores ──────────────────────────────────────────────────────────
hazards: dict = {}
users:   dict = {}
_store_lock = threading.Lock()

DATA_FILE = os.path.join(os.path.dirname(__file__), "..", "data", "hazards.json")
os.makedirs(os.path.dirname(DATA_FILE), exist_ok=True)


# ── Persistence helpers ───────────────────────────────────────────────────────
def _save():
    try:
        with open(DATA_FILE, "w") as f:
            json.dump(hazards, f, indent=2)
    except Exception as e:
        log.warning(f"Could not save data: {e}")


def _load():
    global hazards
    if os.path.exists(DATA_FILE):
        try:
            with open(DATA_FILE) as f:
                hazards = json.load(f)
            log.info(f"Loaded {len(hazards)} hazards from disk.")
        except Exception as e:
            log.warning(f"Could not load data: {e}")


# ── Severity helpers ──────────────────────────────────────────────────────────
def _color(severity: str) -> str:
    return SEVERITY_COLORS.get(severity.lower(), "#888888")


def _expire_old():
    """Remove hazards older than REPORT_EXPIRY_SECONDS."""
    now = time.time()
    with _store_lock:
        stale = [k for k, v in hazards.items()
                 if now - v.get("timestamp", now) > REPORT_EXPIRY_SECONDS]
        for k in stale:
            del hazards[k]
    if stale:
        log.info(f"Expired {len(stale)} old hazards.")
        socketio.emit("hazards_full", {"hazards": list(hazards.values())})


# ── Resolve / delete helper ───────────────────────────────────────────────────
def _remove_hazard(report_id: str) -> bool:
    """
    Delete a hazard by ID. Returns True if it existed.
    Broadcasts 'hazard_removed' to all connected browsers.
    """
    with _store_lock:
        if report_id not in hazards:
            return False
        del hazards[report_id]
    _save()
    log.info(f"Hazard {report_id} resolved/removed.")
    socketio.emit("hazard_removed", {"report_id": report_id})
    return True


# ── Alert logic ───────────────────────────────────────────────────────────────
def _check_alerts_for_user(sid: str, ulat: float, ulon: float):
    nearby = []
    with _store_lock:
        for h in hazards.values():
            dist = haversine((ulat, ulon), (h["lat"], h["lon"]), unit=Unit.METERS)
            if dist <= ALERT_RADIUS_METERS:
                nearby.append({**h, "distance_m": round(dist, 1)})
    if nearby:
        nearby.sort(key=lambda x: x["distance_m"])
        socketio.emit("proximity_alert", {"alerts": nearby}, to=sid)


def _broadcast_alerts_for_hazard(report: dict):
    hlat, hlon = report["lat"], report["lon"]
    with _store_lock:
        snapshot = dict(users)
    for sid, u in snapshot.items():
        dist = haversine((u["lat"], u["lon"]), (hlat, hlon), unit=Unit.METERS)
        if dist <= ALERT_RADIUS_METERS:
            socketio.emit("proximity_alert", {
                "alerts": [{**report, "distance_m": round(dist, 1)}]
            }, to=sid)


# ── MQTT callbacks ────────────────────────────────────────────────────────────
def on_connect(client, userdata, flags, reason_code, properties):
    rc = reason_code
    if rc == 0:
        log.info(f"MQTT connected to {MQTT_BROKER}:{MQTT_PORT}")
        client.subscribe([(TOPIC_REPORT, 0), (TOPIC_LOCATION, 0)])
    else:
        log.error(f"MQTT connection failed rc={rc}")


def on_message(client, userdata, msg):
    try:
        payload = json.loads(msg.payload.decode())
    except json.JSONDecodeError:
        log.warning(f"Bad JSON on {msg.topic}")
        return

    if msg.topic == TOPIC_REPORT:
        _handle_report(payload)
    elif msg.topic == TOPIC_LOCATION:
        _handle_location(payload)


def _handle_report(payload: dict):
    report_id = payload.get("report_id") or str(uuid.uuid4())
    lat = float(payload.get("lat", 0))
    lon = float(payload.get("lon", 0))

    report = {
        "report_id":     report_id,
        "lat":           lat,
        "lon":           lon,
        "category":      payload.get("category", "Other"),
        "severity":      payload.get("severity", "medium"),
        "description":   payload.get("description", ""),
        "user_id":       payload.get("user_id", "anonymous"),
        "reporter_name": payload.get("reporter_name", None),
        "timestamp":     payload.get("timestamp", time.time()),
        "votes":         payload.get("votes", 0),
        "image_b64":     payload.get("image_b64", None),
        "color":         _color(payload.get("severity", "medium")),
        "datetime":      datetime.fromtimestamp(
                             payload.get("timestamp", time.time())
                         ).strftime("%Y-%m-%d %H:%M:%S"),
    }

    with _store_lock:
        hazards[report_id] = report

    _save()
    log.info(f"New hazard [{report['severity'].upper()}] {report['category']} @ ({lat},{lon})")

    socketio.emit("new_hazard", {"hazard": report})
    _broadcast_alerts_for_hazard(report)
    mqtt_client.publish(TOPIC_ACK, json.dumps({"report_id": report_id, "status": "received"}))


def _handle_location(payload: dict):
    user_id = payload.get("user_id", "unknown")
    lat = float(payload.get("lat", 0))
    lon = float(payload.get("lon", 0))
    socketio.emit("user_location_update", {
        "user_id": user_id,
        "lat": lat,
        "lon": lon,
        "timestamp": payload.get("timestamp", time.time()),
    })


# ── SocketIO events ───────────────────────────────────────────────────────────
@socketio.on("connect")
def handle_connect():
    log.info(f"Browser connected: {request.sid}")
    with _store_lock:
        current = list(hazards.values())
    emit("hazards_full", {"hazards": current})


@socketio.on("disconnect")
def handle_disconnect():
    log.info(f"Browser disconnected: {request.sid}")
    with _store_lock:
        users.pop(request.sid, None)


@socketio.on("user_position")
def handle_user_position(data):
    lat = float(data.get("lat", 0))
    lon = float(data.get("lon", 0))
    with _store_lock:
        users[request.sid] = {
            "lat": lat, "lon": lon,
            "user_id": data.get("user_id", request.sid),
            "last_seen": time.time(),
        }
    _check_alerts_for_user(request.sid, lat, lon)


@socketio.on("submit_report")
def handle_submit_report(data):
    data["report_id"] = str(uuid.uuid4())
    data["timestamp"] = time.time()
    data["user_id"] = data.get("user_id", f"web_{request.sid[:6]}")
    # reporter_name is already in data if the browser sent it
    _handle_report(data)


@socketio.on("upvote")
def handle_upvote(data):
    rid = data.get("report_id")
    updated = None
    with _store_lock:
        if rid in hazards:
            hazards[rid]["votes"] = hazards[rid].get("votes", 0) + 1
            updated = hazards[rid]
    if updated:
        _save()
        socketio.emit("hazard_updated", {"hazard": updated})


# ── NEW: Resolve / delete a hazard via SocketIO ───────────────────────────────
@socketio.on("resolve_hazard")
def handle_resolve_hazard(data):
    """
    Client emits: { report_id: "...", reason: "gone" }
    Server removes it and broadcasts hazard_removed to everyone.
    """
    rid = data.get("report_id")
    if not rid:
        emit("error", {"msg": "No report_id provided"})
        return

    removed = _remove_hazard(rid)
    if removed:
        log.info(f"Hazard {rid} resolved by {request.sid}")
    else:
        emit("error", {"msg": f"Hazard {rid} not found"})


# ── REST endpoints ────────────────────────────────────────────────────────────
@app.route("/")
def index():
    return render_template("index.html")


@app.route("/api/hazards")
def api_hazards():
    with _store_lock:
        return jsonify(list(hazards.values()))


# ── NEW: GeoJSON endpoint — paste this URL directly into QGIS ────────────────
@app.route("/api/hazards.geojson")
def api_geojson():
    """
    Returns all current hazards as a GeoJSON FeatureCollection.

    Use in QGIS:
      Layer → Add Layer → Add Vector Layer
      Protocol: HTTP(S)   URI: http://<your-server>:5001/api/hazards.geojson
    """
    with _store_lock:
        features = []
        for h in hazards.values():
            # Strip image data from GeoJSON (too large for GIS tools)
            props = {k: v for k, v in h.items() if k not in ("lat", "lon", "image_b64")}
            features.append({
                "type": "Feature",
                "geometry": {
                    "type": "Point",
                    "coordinates": [h["lon"], h["lat"]],  # GeoJSON is [lon, lat]
                },
                "properties": props,
            })

    geojson = {
        "type": "FeatureCollection",
        "name": "PedSafe Hazards",
        "crs": {
            "type": "name",
            "properties": {"name": "urn:ogc:def:crs:OGC:1.3:CRS84"},
        },
        "features": features,
    }
    resp = jsonify(geojson)
    resp.headers["Content-Type"] = "application/geo+json"
    # Allow QGIS / other GIS clients to fetch without CORS issues
    resp.headers["Access-Control-Allow-Origin"] = "*"
    return resp


@app.route("/api/report", methods=["POST"])
def api_report():
    data = request.get_json(force=True)
    data["report_id"] = str(uuid.uuid4())
    data["timestamp"] = time.time()
    _handle_report(data)
    mqtt_client.publish(TOPIC_REPORT, json.dumps(data))
    return jsonify({"status": "ok", "report_id": data["report_id"]})


# ── NEW: REST endpoint to resolve/delete a hazard ────────────────────────────
@app.route("/api/hazard/<report_id>/resolve", methods=["POST"])
def api_resolve(report_id: str):
    """
    POST /api/hazard/<id>/resolve
    Marks a hazard as resolved and broadcasts removal to all clients.
    Useful for admin scripts, QGIS plugins, or mobile apps.
    """
    removed = _remove_hazard(report_id)
    if removed:
        return jsonify({"status": "resolved", "report_id": report_id})
    return jsonify({"status": "not_found", "report_id": report_id}), 404


@app.route("/api/hazard/<report_id>", methods=["DELETE"])
def api_delete(report_id: str):
    """
    DELETE /api/hazard/<id>
    Standard REST delete — same effect as resolve.
    """
    removed = _remove_hazard(report_id)
    if removed:
        return jsonify({"status": "deleted", "report_id": report_id})
    return jsonify({"status": "not_found", "report_id": report_id}), 404


@app.route("/api/stats")
def api_stats():
    with _store_lock:
        total = len(hazards)
        by_sev = {}
        for h in hazards.values():
            s = h.get("severity", "unknown")
            by_sev[s] = by_sev.get(s, 0) + 1
    return jsonify({"total": total, "by_severity": by_sev, "connected_users": len(users)})


# ── Expiry background thread ──────────────────────────────────────────────────
def _expiry_loop():
    while True:
        time.sleep(300)
        _expire_old()


# ── MQTT client setup ─────────────────────────────────────────────────────────
mqtt_client = mqtt.Client(
    callback_api_version=CallbackAPIVersion.VERSION2,
    client_id=f"ped_safety_server_{uuid.uuid4().hex[:6]}",
)
mqtt_client.on_connect = on_connect
mqtt_client.on_message = on_message


# ── Entry point ───────────────────────────────────────────────────────────────
if __name__ == "__main__":
    _load()
    threading.Thread(target=_expiry_loop, daemon=True).start()

    log.info(f"Connecting to MQTT broker {MQTT_BROKER}:{MQTT_PORT} …")
    try:
        mqtt_client.connect(MQTT_BROKER, MQTT_PORT, MQTT_KEEPALIVE)
        mqtt_client.loop_start()
    except Exception as e:
        log.error(f"MQTT connection failed: {e} — running without MQTT.")

    log.info(f"Starting Flask-SocketIO on http://{FLASK_HOST}:{FLASK_PORT}")
    socketio.run(app, host=FLASK_HOST, port=FLASK_PORT, debug=False)