#!/usr/bin/env python3
"""
qgis_plugin/pedestrian_safety_qgis.py
=======================================
QGIS Plugin — drop this file into:
  ~/Library/Application Support/QGIS/QGIS3/profiles/default/python/plugins/pedestrian_safety/

Then enable it in Plugins > Manage and Install Plugins.

What it does:
  • Polls the Flask REST API every N seconds
  • Renders hazard points as a colour-coded memory layer
  • Shows proximity alerts in the QGIS message bar
"""

import json, time, math
from datetime import datetime

try:
    from qgis.core import (
        QgsVectorLayer, QgsFeature, QgsGeometry, QgsPointXY,
        QgsField, QgsFields, QgsProject, QgsSymbol,
        QgsRendererCategory, QgsCategorizedSymbolRenderer,
        QgsMarkerSymbol, QgsMessageLog, Qgis,
    )
    from qgis.PyQt.QtCore import QVariant, QTimer, Qt
    from qgis.PyQt.QtWidgets import QAction, QMessageBox, QInputDialog
    from qgis.PyQt.QtGui import QColor, QIcon
    from qgis.utils import iface
    QGIS_AVAILABLE = True
except ImportError:
    QGIS_AVAILABLE = False
    print("[PedSafety] Not running inside QGIS — standalone preview mode.")

import urllib.request

SERVER_URL      = "http://127.0.0.1:5000"
POLL_INTERVAL   = 5000   # milliseconds
ALERT_RADIUS_M  = 150

SEVERITY_COLORS = {
    "critical": "#FF0000",
    "high":     "#FF6600",
    "medium":   "#FFD700",
    "low":      "#0099FF",
}


# ─────────────────────────────────────────────────────────────────────────────
class PedestrianSafetyPlugin:
    """QGIS Plugin main class."""

    def __init__(self, iface_obj):
        self.iface    = iface_obj
        self.layer    = None
        self.timer    = None
        self.action   = None
        self.user_lat = None
        self.user_lon = None
        self.plugin_name = "Pedestrian Safety"

    # ── Required QGIS plugin methods ─────────────────────────────────────────
    def initGui(self):
        self.action = QAction("🚶 Pedestrian Safety Monitor", self.iface.mainWindow())
        self.action.triggered.connect(self.run)
        self.iface.addToolBarIcon(self.action)
        self.iface.addPluginToMenu(self.plugin_name, self.action)

    def unload(self):
        self.iface.removeToolBarIcon(self.action)
        self.iface.removePluginMenu(self.plugin_name, self.action)
        if self.timer:
            self.timer.stop()

    # ── Main entry ────────────────────────────────────────────────────────────
    def run(self):
        # Ask user for their location
        lat_str, ok1 = QInputDialog.getText(
            None, "Your Location", "Enter your latitude (e.g. 14.5995):"
        )
        lon_str, ok2 = QInputDialog.getText(
            None, "Your Location", "Enter your longitude (e.g. 120.9842):"
        )
        if ok1 and ok2:
            try:
                self.user_lat = float(lat_str)
                self.user_lon = float(lon_str)
            except ValueError:
                QMessageBox.warning(None, "Error", "Invalid coordinates.")
                return

        self._create_layer()
        self._start_polling()
        self.iface.messageBar().pushMessage(
            "Pedestrian Safety",
            f"Live hazard layer active. Polling every {POLL_INTERVAL//1000}s.",
            level=Qgis.Success, duration=4,
        )

    # ── Layer creation ────────────────────────────────────────────────────────
    def _create_layer(self):
        # Remove old layer if exists
        if self.layer and QgsProject.instance().mapLayer(self.layer.id()):
            QgsProject.instance().removeMapLayer(self.layer.id())

        self.layer = QgsVectorLayer("Point?crs=EPSG:4326", "Hazard Reports", "memory")
        pr = self.layer.dataProvider()

        fields = QgsFields()
        fields.append(QgsField("report_id",   QVariant.String))
        fields.append(QgsField("category",    QVariant.String))
        fields.append(QgsField("severity",    QVariant.String))
        fields.append(QgsField("description", QVariant.String))
        fields.append(QgsField("timestamp",   QVariant.String))
        fields.append(QgsField("votes",       QVariant.Int))
        fields.append(QgsField("user_id",     QVariant.String))
        pr.addAttributes(fields)
        self.layer.updateFields()

        self._apply_renderer()
        QgsProject.instance().addMapLayer(self.layer)

    def _apply_renderer(self):
        """Colour-coded by severity."""
        categories = []
        labels = {
            "critical": "🔴 Critical",
            "high":     "🟠 High",
            "medium":   "🟡 Medium",
            "low":      "🔵 Low",
        }
        for sev, hex_color in SEVERITY_COLORS.items():
            sym = QgsMarkerSymbol.createSimple({
                "name": "circle",
                "color": hex_color,
                "size": "4" if sev in ("critical", "high") else "3",
                "outline_color": "#000000",
                "outline_width": "0.3",
            })
            cat = QgsRendererCategory(sev, sym, labels.get(sev, sev))
            categories.append(cat)

        renderer = QgsCategorizedSymbolRenderer("severity", categories)
        self.layer.setRenderer(renderer)

    # ── Polling ────────────────────────────────────────────────────────────────
    def _start_polling(self):
        self.timer = QTimer()
        self.timer.timeout.connect(self._fetch_and_update)
        self.timer.start(POLL_INTERVAL)
        self._fetch_and_update()   # immediate first fetch

    def _fetch_and_update(self):
        try:
            url = f"{SERVER_URL}/api/hazards"
            with urllib.request.urlopen(url, timeout=4) as resp:
                hazards = json.loads(resp.read().decode())
        except Exception as e:
            QgsMessageLog.logMessage(f"Fetch error: {e}", "PedSafety", Qgis.Warning)
            return

        pr = self.layer.dataProvider()
        pr.truncate()   # clear existing features

        features = []
        alerts   = []
        for h in hazards:
            feat = QgsFeature()
            feat.setGeometry(QgsGeometry.fromPointXY(QgsPointXY(h["lon"], h["lat"])))
            feat.setAttributes([
                h.get("report_id", ""),
                h.get("category", ""),
                h.get("severity", ""),
                h.get("description", ""),
                h.get("datetime", str(h.get("timestamp", ""))),
                int(h.get("votes", 0)),
                h.get("user_id", ""),
            ])
            features.append(feat)

            # proximity check
            if self.user_lat is not None:
                dist = _haversine_m(self.user_lat, self.user_lon, h["lat"], h["lon"])
                if dist <= ALERT_RADIUS_M:
                    alerts.append((h, dist))

        pr.addFeatures(features)
        self.layer.updateExtents()
        self.layer.triggerRepaint()

        # Proximity alerts
        for h, dist in alerts:
            self.iface.messageBar().pushMessage(
                "⚠️ Hazard Nearby!",
                f"{h['category']} ({h['severity'].upper()}) — {dist:.0f} m away: {h['description']}",
                level=Qgis.Critical, duration=6,
            )

        QgsMessageLog.logMessage(
            f"Updated {len(hazards)} hazards at {datetime.now().strftime('%H:%M:%S')}",
            "PedSafety", Qgis.Info,
        )


# ── Standalone haversine (no external lib inside QGIS) ───────────────────────
def _haversine_m(lat1, lon1, lat2, lon2) -> float:
    R = 6_371_000
    φ1, φ2 = math.radians(lat1), math.radians(lat2)
    Δφ = math.radians(lat2 - lat1)
    Δλ = math.radians(lon2 - lon1)
    a = math.sin(Δφ/2)**2 + math.cos(φ1)*math.cos(φ2)*math.sin(Δλ/2)**2
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


# ── QGIS plugin factory ───────────────────────────────────────────────────────
def classFactory(iface_obj):
    return PedestrianSafetyPlugin(iface_obj)


# ── Standalone test (outside QGIS) ───────────────────────────────────────────
if __name__ == "__main__" and not QGIS_AVAILABLE:
    print("Fetching hazards from server …")
    try:
        url = f"{SERVER_URL}/api/hazards"
        with urllib.request.urlopen(url, timeout=4) as resp:
            hazards = json.loads(resp.read().decode())
        print(f"Found {len(hazards)} hazards:")
        for h in hazards:
            print(f"  [{h['severity'].upper():8}] {h['category']:25} @ ({h['lat']}, {h['lon']}) — {h['description']}")
    except Exception as e:
        print(f"Error: {e}")
        print("Make sure the server is running:  python backend/server.py")
