# config/settings.py
# ============================================================
#  Pedestrian Safety System — Configuration
# ============================================================

# MQTT Public Broker (HiveMQ)
MQTT_BROKER   = "broker.hivemq.com"
MQTT_PORT     = 1883
MQTT_KEEPALIVE = 60

# Topics
TOPIC_REPORT   = "pedestrian_safety/hazard_report"
TOPIC_LOCATION = "pedestrian_safety/user_location"
TOPIC_ALERT    = "pedestrian_safety/alert"
TOPIC_ACK      = "pedestrian_safety/ack"

# Alert radius in metres — user gets warned if a hazard is within this distance
ALERT_RADIUS_METERS = 150

# Severity colours
SEVERITY_COLORS = {
    "critical": "#FF0000",   # Red
    "high":     "#FF6600",   # Orange
    "medium":   "#FFD700",   # Yellow
    "low":      "#0099FF",   # Blue
}

# Hazard categories
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

# Flask / SocketIO
FLASK_HOST = "0.0.0.0"
FLASK_PORT = 5001
SECRET_KEY = "pedestrian_safety_secret_2024"

# How long (seconds) before a report is considered stale and faded on map
REPORT_EXPIRY_SECONDS = 3600  # 1 hour
