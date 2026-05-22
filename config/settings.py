# config/settings.py
# ============================================================
#  Pedestrian Safety System — Configuration
#  Production: all secrets come from environment variables.
#  Development: safe defaults are used automatically.
# ============================================================
import os

# ── MQTT Broker ───────────────────────────────────────────────────────────────
# In production (Docker), MQTT runs as a local Mosquitto container.
MQTT_BROKER    = os.getenv("MQTT_BROKER",    "mosquitto")   # Docker service name
MQTT_PORT      = int(os.getenv("MQTT_PORT",  "1883"))
MQTT_KEEPALIVE = int(os.getenv("MQTT_KEEPALIVE", "60"))

# ── MQTT Topics ───────────────────────────────────────────────────────────────
_PREFIX        = os.getenv("MQTT_PREFIX", "pedsafe_v1")
TOPIC_REPORT   = f"{_PREFIX}/hazard_report"
TOPIC_LOCATION = f"{_PREFIX}/user_location"
TOPIC_ALERT    = f"{_PREFIX}/alert"
TOPIC_ACK      = f"{_PREFIX}/ack"

# ── Proximity alert radius ────────────────────────────────────────────────────
ALERT_RADIUS_METERS = int(os.getenv("ALERT_RADIUS_METERS", "150"))

# ── Severity → colour map ─────────────────────────────────────────────────────
SEVERITY_COLORS = {
    "critical": "#FF0000",
    "high":     "#FF6600",
    "medium":   "#FFD700",
    "low":      "#0099FF",
}

# ── Hazard categories ─────────────────────────────────────────────────────────
HAZARD_CATEGORIES = [
    "Pothole",
    "Flood / Standing Water",
    "Debris / Fallen Object",
    "Road Accident",
    "Construction Zone",
    "Broken Pavement",
    "Poor Lighting",
    "Aggressive Animal",
    "Other",
]

# ── Flask / SocketIO ──────────────────────────────────────────────────────────
FLASK_HOST = "0.0.0.0"
FLASK_PORT = int(os.getenv("FLASK_PORT", "5001"))
# CHANGE THIS: set SECRET_KEY env var to a long random string
SECRET_KEY = os.getenv("SECRET_KEY", "CHANGE_ME_IN_PRODUCTION_use_a_long_random_string")

# ── Report lifetime ───────────────────────────────────────────────────────────
REPORT_EXPIRY_SECONDS = int(os.getenv("REPORT_EXPIRY_SECONDS", "3600"))

# ── Rate limiting (max reports per user per minute) ───────────────────────────
RATE_LIMIT_PER_MINUTE = int(os.getenv("RATE_LIMIT_PER_MINUTE", "10"))

# ── Admin password (for /admin page) ─────────────────────────────────────────
ADMIN_PASSWORD = os.getenv("ADMIN_PASSWORD", "changeme")