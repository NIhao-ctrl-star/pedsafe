#!/usr/bin/env python3
"""
pedestrian_safety/backend/server.py
====================================
Flask + Flask-SocketIO server that bridges MQTT ↔ WebSocket.

All hazard reports received from MQTT are:
  1. Stored in memory (+ flat JSON file for persistence)
  2. Broadcast to every connected browser via SocketIO
  3. Checked against connected users' positions → alert if too close

Features:
  • resolve_hazard  SocketIO event  — removes a hazard, broadcasts removal
  • /api/hazards.geojson            — GeoJSON endpoint for QGIS / other GIS tools
  • /api/hazard/<id>  DELETE        — REST endpoint to delete a hazard
  • /api/hazard/<id>/resolve  POST  — REST alias for resolve
  • /api/hazard/<id>/vote     POST  — REST upvote endpoint
  • /api/hazard/<id>/flag     POST  — REST flag endpoint
  • /admin                          — password-protected admin dashboard
  • Rate limiting per user (RATE_LIMIT_PER_MINUTE from settings)
  • Confidence scoring (votes, flags, age, cluster)
  • Cluster-count enrichment for the feed cards
  • MQTT proximity alerts published to TOPIC_ALERT for CLI clients
"""

import sys, os
_HERE = os.path.dirname(os.path.abspath(__file__))
_ROOT = os.path.dirname(_HERE) if os.path.basename(_HERE) == "backend" else _HERE
sys.path.insert(0, _ROOT)

import json, uuid, time, threading, logging, math, functools
from datetime import datetime
from collections import defaultdict

import paho.mqtt.client as mqtt
from paho.mqtt.enums import CallbackAPIVersion
from flask import (
    Flask, render_template, request, jsonify,
    session, redirect, url_for, abort,
)
from flask_socketio import SocketIO, emit
from flask_cors import CORS
from haversine import haversine, Unit

from config.settings import (
    MQTT_BROKER, MQTT_PORT, MQTT_KEEPALIVE,
    TOPIC_REPORT, TOPIC_LOCATION, TOPIC_ALERT, TOPIC_ACK,
    ALERT_RADIUS_METERS, SEVERITY_COLORS,
    FLASK_HOST, FLASK_PORT, SECRET_KEY,
    REPORT_EXPIRY_SECONDS, RATE_LIMIT_PER_MINUTE, ADMIN_PASSWORD,
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
hazards: dict = {}          # report_id → report dict
users:   dict = {}          # socket sid → {lat, lon, user_id, last_seen}
_store_lock = threading.Lock()

# Rate limiting: user_id → list of timestamps
_rate_buckets: dict = defaultdict(list)
_rate_lock = threading.Lock()

DATA_FILE = os.path.join(os.path.dirname(__file__), "..", "data", "hazards.json")
os.makedirs(os.path.dirname(DATA_FILE), exist_ok=True)


# ── Persistence ───────────────────────────────────────────────────────────────
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


# ── Rate limiting ─────────────────────────────────────────────────────────────
def _is_rate_limited(user_id: str) -> bool:
    """Return True if user_id has exceeded RATE_LIMIT_PER_MINUTE."""
    now = time.time()
    window = 60.0
    with _rate_lock:
        bucket = _rate_buckets[user_id]
        # Drop timestamps outside the window
        _rate_buckets[user_id] = [t for t in bucket if now - t < window]
        if len(_rate_buckets[user_id]) >= RATE_LIMIT_PER_MINUTE:
            return True
        _rate_buckets[user_id].append(now)
        return False


# ── Severity helpers ──────────────────────────────────────────────────────────
def _color(severity: str) -> str:
    return SEVERITY_COLORS.get(severity.lower(), "#888888")


# ── Confidence scoring ────────────────────────────────────────────────────────
def _compute_confidence(report: dict) -> int:
    """
    Returns an integer 0-100 representing community confidence.

    Algorithm:
      base        = 50
      + votes     each upvote adds 5, capped at +30
      - flags     each flag subtracts 10, capped at -40
      - age decay starts after 30 min, linearly to -20 at expiry
      + severity  critical/high add 5 (reported with urgency)
    """
    base = 50
    votes = report.get("votes", 0)
    flags = report.get("flags", 0)
    age_s = time.time() - report.get("timestamp", time.time())

    vote_bonus   = min(votes * 5, 30)
    flag_penalty = min(flags * 10, 40)

    # Decay: 0 for first 30 min, then linear to -20 at REPORT_EXPIRY_SECONDS
    decay_start = 1800  # 30 minutes
    decay = 0
    if age_s > decay_start and REPORT_EXPIRY_SECONDS > decay_start:
        ratio = (age_s - decay_start) / (REPORT_EXPIRY_SECONDS - decay_start)
        decay = int(min(ratio, 1.0) * 20)

    sev_bonus = 5 if report.get("severity") in ("critical", "high") else 0

    score = base + vote_bonus - flag_penalty - decay + sev_bonus
    return max(0, min(100, score))


# ── Cluster enrichment ────────────────────────────────────────────────────────
CLUSTER_RADIUS_M = 50   # group hazards within 50 m for the cluster_count field

def _enrich_cluster_counts():
    """
    For every hazard, count how many other hazards are within CLUSTER_RADIUS_M.
    Updates hazards in-place (must be called while holding _store_lock).
    """
    items = list(hazards.values())
    for h in items:
        count = sum(
            1 for o in items
            if o["report_id"] != h["report_id"]
            and haversine(
                (h["lat"], h["lon"]), (o["lat"], o["lon"]), unit=Unit.METERS
            ) <= CLUSTER_RADIUS_M
        )
        h["cluster_count"] = count + 1   # include self


# ── Expiry ────────────────────────────────────────────────────────────────────
def _expire_old():
    """Remove hazards older than REPORT_EXPIRY_SECONDS."""
    now = time.time()
    with _store_lock:
        stale = [
            k for k, v in hazards.items()
            if now - v.get("timestamp", now) > REPORT_EXPIRY_SECONDS
        ]
        for k in stale:
            del hazards[k]
    if stale:
        log.info(f"Expired {len(stale)} old hazard(s).")
        socketio.emit("hazards_full", {"hazards": _snapshot()})


def _expiry_loop():
    while True:
        time.sleep(300)
        _expire_old()


# ── Snapshot helper ───────────────────────────────────────────────────────────
def _snapshot() -> list:
    """Return a list of all hazards enriched with confidence and cluster_count."""
    with _store_lock:
        _enrich_cluster_counts()
        result = []
        for h in hazards.values():
            h["confidence"] = _compute_confidence(h)
            result.append(dict(h))
    return result


# ── Resolve / delete helper ───────────────────────────────────────────────────
def _remove_hazard(report_id: str) -> bool:
    """
    Delete a hazard by ID.  Returns True if it existed.
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
    """
    Send proximity_alert to a specific browser session (by sid) for any
    hazard within ALERT_RADIUS_METERS.  Also publishes to MQTT TOPIC_ALERT
    so CLI clients (mqtt_client.py) receive the same notifications.
    """
    nearby = []
    with _store_lock:
        for h in hazards.values():
            dist = haversine((ulat, ulon), (h["lat"], h["lon"]), unit=Unit.METERS)
            if dist <= ALERT_RADIUS_METERS:
                nearby.append({**h, "distance_m": round(dist, 1)})

    if nearby:
        nearby.sort(key=lambda x: x["distance_m"])
        socketio.emit("proximity_alert", {"alerts": nearby}, to=sid)

        # Also push to MQTT so CLI listeners receive it
        user_id = users.get(sid, {}).get("user_id", sid)
        try:
            mqtt_client.publish(
                TOPIC_ALERT,
                json.dumps({"user_id": user_id, "alerts": nearby}),
            )
        except Exception as e:
            log.debug(f"MQTT alert publish failed: {e}")


def _broadcast_alerts_for_hazard(report: dict):
    """
    When a new hazard arrives, alert every browser user already within range.
    Also publishes an MQTT alert for each affected CLI user.
    """
    hlat, hlon = report["lat"], report["lon"]
    with _store_lock:
        snapshot = dict(users)

    for sid, u in snapshot.items():
        dist = haversine((u["lat"], u["lon"]), (hlat, hlon), unit=Unit.METERS)
        if dist <= ALERT_RADIUS_METERS:
            alert_payload = {**report, "distance_m": round(dist, 1)}
            socketio.emit(
                "proximity_alert",
                {"alerts": [alert_payload]},
                to=sid,
            )
            # Mirror to MQTT for CLI clients
            try:
                mqtt_client.publish(
                    TOPIC_ALERT,
                    json.dumps({"user_id": u["user_id"], "alerts": [alert_payload]}),
                )
            except Exception as e:
                log.debug(f"MQTT broadcast alert failed: {e}")


# ── MQTT callbacks ────────────────────────────────────────────────────────────
def on_connect(client, userdata, flags, reason_code, properties=None):
    rc = reason_code if isinstance(reason_code, int) else reason_code.value
    if rc == 0:
        log.info(f"MQTT connected to {MQTT_BROKER}:{MQTT_PORT}")
        client.subscribe([
            (TOPIC_REPORT,   0),
            (TOPIC_LOCATION, 0),
        ])
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
    user_id   = payload.get("user_id", "anonymous")
    report_id = payload.get("report_id") or str(uuid.uuid4())

    if _is_rate_limited(user_id):
        log.warning(f"Rate limit hit for user {user_id}")
        return

    lat = float(payload.get("lat", 0))
    lon = float(payload.get("lon", 0))

    report = {
        "report_id":     report_id,
        "lat":           lat,
        "lon":           lon,
        "category":      payload.get("category", "Other"),
        "severity":      payload.get("severity", "medium"),
        "description":   payload.get("description", ""),
        "user_id":       user_id,
        "reporter_name": payload.get("reporter_name", None),
        "timestamp":     payload.get("timestamp", time.time()),
        "votes":         int(payload.get("votes", 0)),
        "flags":         int(payload.get("flags", 0)),
        "image_b64":     payload.get("image_b64", None),
        "color":         _color(payload.get("severity", "medium")),
        "datetime":      datetime.fromtimestamp(
                             payload.get("timestamp", time.time())
                         ).strftime("%Y-%m-%d %H:%M:%S"),
        "cluster_count": 1,
        "confidence":    50,
    }

    with _store_lock:
        hazards[report_id] = report
        _enrich_cluster_counts()

    report["confidence"] = _compute_confidence(report)
    _save()
    log.info(
        f"New hazard [{report['severity'].upper()}] "
        f"{report['category']} @ ({lat:.5f},{lon:.5f})"
    )

    socketio.emit("new_hazard", {"hazard": dict(report)})
    _broadcast_alerts_for_hazard(report)
    try:
        mqtt_client.publish(
            TOPIC_ACK,
            json.dumps({"report_id": report_id, "status": "received"}),
        )
    except Exception as e:
        log.debug(f"ACK publish failed: {e}")


def _handle_location(payload: dict):
    """
    Process a location update from an MQTT client (e.g. mqtt_client.py).
    Updates the in-memory users store so proximity checks work for MQTT users,
    then broadcasts the position to browser clients.
    """
    user_id = payload.get("user_id", "unknown")
    lat     = float(payload.get("lat", 0))
    lon     = float(payload.get("lon", 0))
    ts      = payload.get("timestamp", time.time())

    # Persist MQTT-sourced positions in the users store so that
    # _broadcast_alerts_for_hazard can reach them when new hazards arrive.
    # We use the user_id as the key (not a socket sid) with a sentinel sid
    # so the dict stays consistent with the SocketIO-sourced entries.
    mqtt_sid = f"mqtt_{user_id}"
    with _store_lock:
        users[mqtt_sid] = {
            "lat":       lat,
            "lon":       lon,
            "user_id":   user_id,
            "last_seen": ts,
        }

    # Check whether this MQTT user is near any existing hazard, then push
    # an MQTT alert directly (they have no WebSocket session).
    nearby = []
    with _store_lock:
        for h in hazards.values():
            dist = haversine((lat, lon), (h["lat"], h["lon"]), unit=Unit.METERS)
            if dist <= ALERT_RADIUS_METERS:
                nearby.append({**h, "distance_m": round(dist, 1)})

    if nearby:
        nearby.sort(key=lambda x: x["distance_m"])
        try:
            mqtt_client.publish(
                TOPIC_ALERT,
                json.dumps({"user_id": user_id, "alerts": nearby}),
            )
        except Exception as e:
            log.debug(f"MQTT location-based alert failed: {e}")

    # Broadcast to all browser clients so live user-trail markers update.
    socketio.emit("user_location_update", {
        "user_id":   user_id,
        "lat":       lat,
        "lon":       lon,
        "timestamp": ts,
    })


# ── SocketIO events ───────────────────────────────────────────────────────────
@socketio.on("connect")
def handle_connect():
    log.info(f"Browser connected: {request.sid}")
    emit("hazards_full", {"hazards": _snapshot()})


@socketio.on("disconnect")
def handle_disconnect():
    log.info(f"Browser disconnected: {request.sid}")
    with _store_lock:
        users.pop(request.sid, None)


@socketio.on("user_position")
def handle_user_position(data):
    """Browser sends its GPS position so the server can send proximity alerts."""
    lat = float(data.get("lat", 0))
    lon = float(data.get("lon", 0))
    with _store_lock:
        users[request.sid] = {
            "lat":       lat,
            "lon":       lon,
            "user_id":   data.get("user_id", request.sid),
            "last_seen": time.time(),
        }
    _check_alerts_for_user(request.sid, lat, lon)


@socketio.on("submit_report")
def handle_submit_report(data):
    data["report_id"] = str(uuid.uuid4())
    data["timestamp"] = time.time()
    data["user_id"]   = data.get("user_id", f"web_{request.sid[:6]}")
    _handle_report(data)


@socketio.on("upvote")
def handle_upvote(data):
    rid     = data.get("report_id")
    updated = None
    with _store_lock:
        if rid in hazards:
            hazards[rid]["votes"]      = hazards[rid].get("votes", 0) + 1
            hazards[rid]["confidence"] = _compute_confidence(hazards[rid])
            updated = dict(hazards[rid])
    if updated:
        _save()
        socketio.emit("hazard_updated", {"hazard": updated})
    else:
        emit("error", {"msg": f"Hazard {rid} not found"})


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
        log.info(f"Hazard {rid} resolved by browser {request.sid}")
    else:
        emit("error", {"msg": f"Hazard {rid} not found"})


@socketio.on("flag_report")
def handle_flag_report(data):
    rid     = data.get("report_id")
    updated = None
    with _store_lock:
        if rid in hazards:
            hazards[rid]["flags"]      = hazards[rid].get("flags", 0) + 1
            hazards[rid]["confidence"] = _compute_confidence(hazards[rid])
            updated = dict(hazards[rid])
    if updated:
        _save()
        socketio.emit("hazard_updated", {"hazard": updated})
        # Auto-remove if confidence hits 0 and heavily flagged
        if updated["confidence"] == 0 and updated.get("flags", 0) >= 5:
            log.info(f"Auto-removing heavily flagged hazard {rid}")
            _remove_hazard(rid)
    else:
        emit("error", {"msg": f"Hazard {rid} not found"})


@socketio.on("offline_flush")
def handle_offline_flush(data):
    """
    Browser sends a batch of reports collected while offline.
    Each is processed through _handle_report normally.
    """
    reports = data.get("reports", [])
    count   = 0
    for r in reports:
        try:
            r["report_id"] = str(uuid.uuid4())
            r["timestamp"] = time.time()
            r.setdefault("user_id", f"web_{request.sid[:6]}")
            _handle_report(r)
            count += 1
        except Exception as e:
            log.warning(f"Offline flush error: {e}")
    emit("offline_flush_ack", {"count": count})


# ── Admin auth decorator ──────────────────────────────────────────────────────
def _admin_required(f):
    @functools.wraps(f)
    def wrapper(*args, **kwargs):
        if not session.get("admin_authed"):
            return redirect(url_for("admin_login"))
        return f(*args, **kwargs)
    return wrapper


# ── REST endpoints ────────────────────────────────────────────────────────────
@app.route("/")
def index():
    return render_template("index.html")


@app.route("/api/hazards")
def api_hazards():
    return jsonify(_snapshot())


@app.route("/api/hazards.geojson")
def api_geojson():
    """
    Returns all current hazards as a GeoJSON FeatureCollection.

    Query params:
      ?severity=critical
      ?since=<unix_ts>
      ?bbox=minlon,minlat,maxlon,maxlat

    Use in QGIS:
      Layer → Add Layer → Add Vector Layer
      Protocol: HTTP(S)   URI: http://<your-server>:5001/api/hazards.geojson
    """
    sev_filter = request.args.get("severity")
    since_ts   = request.args.get("since", type=float)
    bbox_raw   = request.args.get("bbox")
    bbox = None
    if bbox_raw:
        try:
            bbox = list(map(float, bbox_raw.split(",")))
        except ValueError:
            bbox = None

    with _store_lock:
        features = []
        for h in hazards.values():
            if sev_filter and h.get("severity") != sev_filter:
                continue
            if since_ts and h.get("timestamp", 0) < since_ts:
                continue
            if bbox:
                minlon, minlat, maxlon, maxlat = bbox
                if not (minlat <= h["lat"] <= maxlat and minlon <= h["lon"] <= maxlon):
                    continue
            props = {
                k: v for k, v in h.items()
                if k not in ("lat", "lon", "image_b64")
            }
            props["confidence"] = _compute_confidence(h)
            features.append({
                "type": "Feature",
                "geometry": {
                    "type": "Point",
                    "coordinates": [h["lon"], h["lat"]],
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
    resp.headers["Access-Control-Allow-Origin"] = "*"
    return resp


@app.route("/api/report", methods=["POST"])
def api_report():
    """REST endpoint to submit a hazard report (bypasses MQTT)."""
    data = request.get_json(force=True)
    data["report_id"] = str(uuid.uuid4())
    data["timestamp"] = time.time()
    _handle_report(data)
    # Mirror to MQTT so other subscribers (e.g. logging) see it
    try:
        mqtt_client.publish(TOPIC_REPORT, json.dumps(data))
    except Exception as e:
        log.warning(f"MQTT publish failed: {e}")
    return jsonify({"status": "ok", "report_id": data["report_id"]})


@app.route("/api/hazard/<report_id>/resolve", methods=["POST"])
def api_resolve(report_id: str):
    """Mark a hazard as resolved and remove it."""
    removed = _remove_hazard(report_id)
    if removed:
        return jsonify({"status": "resolved", "report_id": report_id})
    return jsonify({"status": "not_found", "report_id": report_id}), 404


@app.route("/api/hazard/<report_id>", methods=["DELETE"])
def api_delete(report_id: str):
    """Delete a hazard (alias for resolve, REST-style)."""
    removed = _remove_hazard(report_id)
    if removed:
        return jsonify({"status": "deleted", "report_id": report_id})
    return jsonify({"status": "not_found", "report_id": report_id}), 404


@app.route("/api/hazard/<report_id>/vote", methods=["POST"])
def api_vote(report_id: str):
    """
    REST upvote endpoint — increments votes and recomputes confidence.
    Body (optional): { "delta": 1 }   (use -1 to undo a vote)
    Broadcasts hazard_updated to all browsers.
    """
    body  = request.get_json(force=True, silent=True) or {}
    delta = int(body.get("delta", 1))

    updated = None
    with _store_lock:
        if report_id in hazards:
            hazards[report_id]["votes"] = max(
                0, hazards[report_id].get("votes", 0) + delta
            )
            hazards[report_id]["confidence"] = _compute_confidence(hazards[report_id])
            updated = dict(hazards[report_id])

    if updated:
        _save()
        socketio.emit("hazard_updated", {"hazard": updated})
        return jsonify({"status": "ok", "votes": updated["votes"],
                        "confidence": updated["confidence"]})
    return jsonify({"status": "not_found", "report_id": report_id}), 404


@app.route("/api/hazard/<report_id>/flag", methods=["POST"])
def api_flag(report_id: str):
    """
    REST flag endpoint — increments flags, recomputes confidence, and
    auto-removes the hazard if it becomes zero-confidence with ≥5 flags.
    Broadcasts hazard_updated (or hazard_removed) to all browsers.
    """
    updated = None
    with _store_lock:
        if report_id in hazards:
            hazards[report_id]["flags"]      = hazards[report_id].get("flags", 0) + 1
            hazards[report_id]["confidence"] = _compute_confidence(hazards[report_id])
            updated = dict(hazards[report_id])

    if not updated:
        return jsonify({"status": "not_found", "report_id": report_id}), 404

    _save()
    socketio.emit("hazard_updated", {"hazard": updated})

    # Auto-remove heavily flagged, zero-confidence hazards
    if updated["confidence"] == 0 and updated.get("flags", 0) >= 5:
        log.info(f"Auto-removing heavily flagged hazard {report_id}")
        _remove_hazard(report_id)
        return jsonify({
            "status": "auto_removed",
            "report_id": report_id,
            "flags": updated["flags"],
        })

    return jsonify({
        "status": "ok",
        "flags": updated["flags"],
        "confidence": updated["confidence"],
    })


@app.route("/api/hazards/heatmap")
def api_heatmap():
    """Returns lat/lon/intensity points for the Leaflet heatmap layer."""
    SEV_WEIGHT = {"critical": 1.0, "high": 0.75, "medium": 0.5, "low": 0.25}
    with _store_lock:
        points = [
            [h["lat"], h["lon"], SEV_WEIGHT.get(h.get("severity", "low"), 0.3)]
            for h in hazards.values()
        ]
    return jsonify({"points": points})


@app.route("/api/route", methods=["POST"])
def api_route():
    """
    Pedestrian route via OSRM public API.
    Requests alternatives and selects the best route that avoids hazards.
    Body: { from: [lat,lon], to: [lat,lon], avoid_severity: ["critical","high"] }
    """
    import urllib.request

    data      = request.get_json(force=True)
    frm       = data.get("from")
    to        = data.get("to")
    avoid_sev = set(data.get("avoid_severity", ["critical", "high"]))

    if not frm or not to:
        return jsonify({"error": "from and to are required"}), 400

    coords = f"{frm[1]},{frm[0]};{to[1]},{to[0]}"
    
    # Request alternatives from OSRM to find workarounds
    url = (
        f"http://router.project-osrm.org/route/v1/foot/{coords}"
        f"?overview=full&geometries=geojson&alternatives=true"
    )

    try:
        with urllib.request.urlopen(url, timeout=8) as resp:
            osrm = json.loads(resp.read())
    except Exception as e:
        return jsonify({
            "error":            f"OSRM unavailable: {e}",
            "route":            None,
            "distance_m":       None,
            "duration_s":       None,
            "hazards_on_route": [],
            "warning": (
                "Could not reach routing service — check internet access."
            ),
        }), 200

    if not osrm.get("routes"):
        return jsonify({"error": "No route found"}), 404

    SNAP_M = 50
    # Average pedestrian walk speed: 1.3 meters/second (~4.7 km/h)
    WALKING_SPEED_MPS = 1.3 

    best_route = None
    lowest_penalty = float('inf')
    best_route_hazards = []

    with _store_lock:
        # Evaluate all alternative routes
        for route in osrm["routes"]:
            geom        = route["geometry"]
            dist_m      = route["distance"]
            coords_list = geom["coordinates"]   # [[lon, lat], …]

            route_hazards = []
            penalty_score = dist_m  # Base cost is the actual walking distance

            # Check this route against all active hazards
            for h in hazards.values():
                # Only penalize if it's a severity level the user wants to avoid
                if h.get("severity") not in avoid_sev:
                    continue

                for lon_c, lat_c in coords_list[::5]:
                    d = haversine(
                        (h["lat"], h["lon"]), (lat_c, lon_c), unit=Unit.METERS
                    )
                    if d <= SNAP_M:
                        route_hazards.append(h)
                        # Massive penalty for intersecting a hazard we want to avoid
                        penalty_score += 10000 
                        break

            # If this is the safest/shortest route we've seen, save it
            if penalty_score < lowest_penalty:
                lowest_penalty = penalty_score
                best_route = route
                best_route_hazards = route_hazards

    # Recalculate duration manually using our custom walking speed
    final_distance_m = best_route["distance"]
    final_duration_s = round(final_distance_m / WALKING_SPEED_MPS)

    warning = None
    if best_route_hazards:
        cats    = ", ".join({h["category"] for h in best_route_hazards})
        warning = (
            f"Could not entirely avoid hazards. {len(best_route_hazards)} hazard(s) "
            f"remain on the safest alternative route: {cats}."
        )

    return jsonify({
        "route":            best_route["geometry"],
        "distance_m":       final_distance_m,
        "duration_s":       final_duration_s,
        "hazards_on_route": best_route_hazards,
        "warning":          warning,
    })


@app.route("/api/stats")
def api_stats():
    """Aggregate statistics consumed by the browser status bar."""
    with _store_lock:
        total    = len(hazards)
        by_sev   = {}
        by_cat   = {}
        avg_conf = 0
        for h in hazards.values():
            s = h.get("severity", "unknown")
            by_sev[s] = by_sev.get(s, 0) + 1
            c = h.get("category", "Other")
            by_cat[c] = by_cat.get(c, 0) + 1
            avg_conf += _compute_confidence(h)
        if total:
            avg_conf = round(avg_conf / total)
    return jsonify({
        "total":           total,
        "by_severity":     by_sev,
        "by_category":     by_cat,
        "avg_confidence":  avg_conf,
        "connected_users": len(users),
    })


# ── Admin endpoints ───────────────────────────────────────────────────────────
@app.route("/admin/login", methods=["GET", "POST"])
def admin_login():
    error = None
    if request.method == "POST":
        if request.form.get("password") == ADMIN_PASSWORD:
            session["admin_authed"] = True
            return redirect(url_for("admin_dashboard"))
        error = "Incorrect password."
    return (
        f"""<!doctype html><html><head><title>PedSafe Admin Login</title>
        <style>body{{font-family:sans-serif;background:#0a0e1a;color:#e2e8f0;
          display:flex;align-items:center;justify-content:center;height:100vh;margin:0}}
        form{{background:#111827;padding:32px;border-radius:12px;border:1px solid #1e2d45;min-width:280px}}
        h2{{margin:0 0 20px;font-size:1.2rem}}
        input{{width:100%;padding:10px;background:#1a2236;border:1px solid #1e2d45;
          border-radius:8px;color:#e2e8f0;font-size:.9rem;margin-bottom:12px;box-sizing:border-box}}
        button{{width:100%;padding:11px;background:#38bdf8;border:none;border-radius:8px;
          color:#000;font-weight:700;cursor:pointer;font-size:.9rem}}
        .err{{color:#ef4444;font-size:.8rem;margin-bottom:10px}}</style></head>
        <body><form method="post">
          <h2>🔐 PedSafe Admin</h2>
          {"<div class='err'>"+error+"</div>" if error else ""}
          <input type="password" name="password" placeholder="Admin password" autofocus/>
          <button type="submit">Login</button>
        </form></body></html>""",
        200,
    )


@app.route("/admin/logout")
def admin_logout():
    session.pop("admin_authed", None)
    return redirect(url_for("admin_login"))


@app.route("/admin")
@_admin_required
def admin_dashboard():
    """Password-protected admin dashboard — lists all hazards with actions."""
    with _store_lock:
        snap = sorted(hazards.values(), key=lambda h: -h.get("timestamp", 0))

    rows = ""
    sev_colors = {
        "critical": "#ef4444",
        "high":     "#f97316",
        "medium":   "#eab308",
        "low":      "#3b82f6",
    }
    for h in snap:
        conf    = _compute_confidence(h)
        age     = int(time.time() - h.get("timestamp", time.time()))
        age_str = (
            f"{age // 3600}h {(age % 3600) // 60}m"
            if age >= 3600
            else f"{age // 60}m {age % 60}s"
        )
        sc = sev_colors.get(h.get("severity"), "#888")
        rows += f"""<tr>
          <td style="font-family:monospace;font-size:.72rem;color:#94a3b8">{h['report_id'][:12]}…</td>
          <td><span style="color:{sc};font-weight:700">{h.get('severity','?').upper()}</span></td>
          <td>{h.get('category','?')}</td>
          <td style="font-family:monospace;font-size:.75rem">{h['lat']:.4f}, {h['lon']:.4f}</td>
          <td style="text-align:center">{h.get('votes',0)} 👍 / {h.get('flags',0)} 🏳️</td>
          <td style="text-align:center">{conf}%</td>
          <td style="color:#94a3b8;font-size:.8rem">{age_str}</td>
          <td>{h.get('datetime','')}</td>
          <td>
            <form method="post" action="/admin/resolve/{h['report_id']}" style="display:inline">
              <button style="background:#22c55e22;border:1px solid #22c55e44;color:#22c55e;
                border-radius:6px;padding:3px 10px;cursor:pointer;font-size:.75rem">✅ Resolve</button>
            </form>
          </td>
        </tr>"""

    total_hazards = len(snap)
    critical_count = sum(1 for h in snap if h.get("severity") == "critical")

    return f"""<!doctype html><html><head><title>PedSafe Admin</title>
    <style>
      body{{font-family:'Segoe UI',sans-serif;background:#0a0e1a;color:#e2e8f0;margin:0;padding:20px}}
      h1{{font-size:1.4rem;margin:0 0 6px}}
      p{{color:#94a3b8;font-size:.85rem;margin:0 0 20px}}
      table{{width:100%;border-collapse:collapse;font-size:.82rem}}
      th{{background:#111827;padding:10px 12px;text-align:left;color:#94a3b8;
          font-size:.72rem;text-transform:uppercase;letter-spacing:.06em;border-bottom:1px solid #1e2d45}}
      td{{padding:9px 12px;border-bottom:1px solid #111827;vertical-align:middle}}
      tr:hover td{{background:#111827}}
      a{{color:#38bdf8;text-decoration:none;font-size:.82rem}}
      .logout{{float:right;background:#ef444422;border:1px solid #ef444444;color:#ef4444;
        border-radius:6px;padding:5px 14px;cursor:pointer;font-size:.8rem;text-decoration:none}}
      .danger-btn{{background:#ef444422;border:1px solid #ef444444;color:#ef4444;
        border-radius:6px;padding:6px 16px;cursor:pointer;font-size:.8rem;margin-left:10px}}
      .stat-chips{{display:flex;gap:12px;margin-bottom:18px;flex-wrap:wrap}}
      .chip{{background:#111827;border:1px solid #1e2d45;border-radius:8px;
        padding:10px 18px;font-size:.82rem}}
      .chip strong{{display:block;font-size:1.3rem;font-weight:700;color:#38bdf8}}
    </style></head>
    <body>
      <h1>🛡️ PedSafe Admin Dashboard</h1>
      <p>
        <a href="/">← Back to map</a>
        <a class="logout" href="/admin/logout">Logout</a>
      </p>

      <!-- Quick stats -->
      <div class="stat-chips">
        <div class="chip"><strong>{total_hazards}</strong>Active Hazards</div>
        <div class="chip"><strong style="color:#ef4444">{critical_count}</strong>Critical</div>
        <div class="chip"><strong>{len(users)}</strong>Connected Users</div>
      </div>

      <!-- Danger zone: clear all -->
      <form method="post" action="/admin/clear_all"
            onsubmit="return confirm('Delete ALL {total_hazards} hazard(s)? This cannot be undone.');"
            style="margin-bottom:16px">
        <button type="submit" class="danger-btn">🗑️ Clear All Hazards</button>
      </form>

      <table>
        <tr>
          <th>ID</th><th>Severity</th><th>Category</th><th>Coords</th>
          <th>Votes / Flags</th><th>Confidence</th><th>Age</th><th>Reported</th><th>Action</th>
        </tr>
        {rows if rows else
          "<tr><td colspan='9' style='text-align:center;color:#94a3b8;padding:40px'>"
          "No active hazards</td></tr>"}
      </table>
    </body></html>"""


@app.route("/admin/resolve/<report_id>", methods=["POST"])
@_admin_required
def admin_resolve(report_id: str):
    _remove_hazard(report_id)
    return redirect(url_for("admin_dashboard"))


@app.route("/admin/clear_all", methods=["POST"])
@_admin_required
def admin_clear_all():
    """Remove ALL hazards — nuclear option for testing / emergency reset."""
    with _store_lock:
        ids = list(hazards.keys())
        hazards.clear()
    _save()
    log.warning(f"Admin cleared ALL {len(ids)} hazards.")
    socketio.emit("hazards_full", {"hazards": []})
    return redirect(url_for("admin_dashboard"))


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
    
    def _broadcast_alerts_for_hazard(self, hazard):
        """
        Broadcasts a newly reported hazard to all connected Socket.IO clients.
        Frontend clients should listen for the 'hazard_alert' event.
        """
        try:
            # Emit the new hazard data to all connected clients
            socketio.emit('hazard_alert', hazard)
            
            # Optional: Log the broadcast for server-side monitoring
            hazard_id = hazard.get('id', 'Unknown')
            severity = hazard.get('severity', 'Unknown')
            print(f"[ALERT] Broadcasted {severity} hazard ({hazard_id}) to all clients.")
            
        except Exception as e:
            print(f"[ERROR] Failed to broadcast hazard alert: {e}")

def _broadcast_alerts_for_hazard(self, hazard):
        """
        Broadcasts a newly reported hazard to all connected Socket.IO clients.
        Frontend clients should listen for the 'hazard_alert' event.
        """
        try:
            # Emit the new hazard data to all connected clients
            socketio.emit('hazard_alert', hazard)
            
            # Optional: Log the broadcast for server-side monitoring
            hazard_id = hazard.get('id', 'Unknown')
            severity = hazard.get('severity', 'Unknown')
            print(f"[ALERT] Broadcasted {severity} hazard ({hazard_id}) to all clients.")
            
        except Exception as e:
            print(f"[ERROR] Failed to broadcast hazard alert: {e}")

def _broadcast_alerts_for_hazard(self, hazard):
        """
        Broadcasts a newly reported hazard to all connected Socket.IO clients.
        Frontend clients should listen for the 'hazard_alert' event.
        """
        try:
            # Emit the new hazard data to all connected clients
            socketio.emit('hazard_alert', hazard)
            
            # Optional: Log the broadcast for server-side monitoring
            hazard_id = hazard.get('id', 'Unknown')
            severity = hazard.get('severity', 'Unknown')
            print(f"[ALERT] Broadcasted {severity} hazard ({hazard_id}) to all clients.")
            
        except Exception as e:
            print(f"[ERROR] Failed to broadcast hazard alert: {e}")