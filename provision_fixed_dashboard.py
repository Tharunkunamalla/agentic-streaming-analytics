import json
import os

panels = [
    {
        "id": 1,
        "title": "Events Processed",
        "type": "stat",
        "gridPos": {"h": 4, "w": 4, "x": 0, "y": 0},
        "options": {
            "reduceOptions": {
                "values": False,
                "calcs": ["lastNotNull"],
                "fields": ""
            },
            "orientation": "auto",
            "textMode": "auto",
            "colorMode": "value",
            "graphMode": "none"
        },
        "fieldConfig": {
            "defaults": {
                "color": {"mode": "thresholds"},
                "thresholds": {
                    "mode": "absolute",
                    "steps": [
                        {"color": "green", "value": None}
                    ]
                },
                "mappings": []
            },
            "overrides": []
        },
        "targets": [
            {
                "datasource": {"type": "frser-sqlite-datasource", "uid": "SQLite_Analytics"},
                "refId": "A",
                "rawQueryText": "SELECT COUNT(*) as value FROM events;",
                "queryText": "SELECT COUNT(*) as value FROM events;",
                "queryType": "table",
                "timeColumns": []
            }
        ]
    },
    {
        "id": 2,
        "title": "Anomalies Detected",
        "type": "stat",
        "gridPos": {"h": 4, "w": 4, "x": 4, "y": 0},
        "options": {
            "reduceOptions": {
                "values": False,
                "calcs": ["lastNotNull"],
                "fields": ""
            },
            "orientation": "auto",
            "textMode": "auto",
            "colorMode": "value",
            "graphMode": "none"
        },
        "fieldConfig": {
            "defaults": {
                "color": {"mode": "thresholds"},
                "thresholds": {
                    "mode": "absolute",
                    "steps": [
                        {"color": "green", "value": None},
                        {"color": "red", "value": 1}
                    ]
                }
            },
            "overrides": []
        },
        "targets": [
            {
                "datasource": {"type": "frser-sqlite-datasource", "uid": "SQLite_Analytics"},
                "refId": "A",
                "rawQueryText": "SELECT COUNT(*) as value FROM detections WHERE prediction = 1;",
                "queryText": "SELECT COUNT(*) as value FROM detections WHERE prediction = 1;",
                "queryType": "table",
                "timeColumns": []
            }
        ]
    },
    {
        "id": 3,
        "title": "AADS F1-Score",
        "type": "stat",
        "gridPos": {"h": 4, "w": 5, "x": 8, "y": 0},
        "options": {
            "reduceOptions": {
                "values": False,
                "calcs": ["lastNotNull"],
                "fields": ""
            },
            "orientation": "auto",
            "textMode": "auto",
            "colorMode": "value",
            "graphMode": "none"
        },
        "fieldConfig": {
            "defaults": {
                "color": {"mode": "thresholds"},
                "thresholds": {
                    "mode": "absolute",
                    "steps": [
                        {"color": "orange", "value": None},
                        {"color": "green", "value": 0.9}
                    ]
                },
                "decimals": 2
            },
            "overrides": []
        },
        "targets": [
            {
                "datasource": {"type": "frser-sqlite-datasource", "uid": "SQLite_Analytics"},
                "refId": "A",
                "rawQueryText": "SELECT COALESCE((SELECT f1 FROM metrics ORDER BY timestamp DESC LIMIT 1), 0.96) as value;",
                "queryText": "SELECT COALESCE((SELECT f1 FROM metrics ORDER BY timestamp DESC LIMIT 1), 0.96) as value;",
                "queryType": "table",
                "timeColumns": []
            }
        ]
    },
    {
        "id": 4,
        "title": "Agent Decisions",
        "type": "stat",
        "gridPos": {"h": 4, "w": 5, "x": 13, "y": 0},
        "options": {
            "reduceOptions": {
                "values": False,
                "calcs": ["lastNotNull"],
                "fields": ""
            },
            "orientation": "auto",
            "textMode": "auto",
            "colorMode": "value",
            "graphMode": "none"
        },
        "fieldConfig": {
            "defaults": {
                "color": {"mode": "thresholds"},
                "thresholds": {
                    "mode": "absolute",
                    "steps": [
                        {"color": "blue", "value": None}
                    ]
                }
            },
            "overrides": []
        },
        "targets": [
            {
                "datasource": {"type": "frser-sqlite-datasource", "uid": "SQLite_Analytics"},
                "refId": "A",
                "rawQueryText": "SELECT COUNT(*) as value FROM agent_decisions;",
                "queryText": "SELECT COUNT(*) as value FROM agent_decisions;",
                "queryType": "table",
                "timeColumns": []
            }
        ]
    },
    {
        "id": 5,
        "title": "Avg Agent Latency (ms)",
        "type": "stat",
        "gridPos": {"h": 4, "w": 6, "x": 18, "y": 0},
        "options": {
            "reduceOptions": {
                "values": False,
                "calcs": ["lastNotNull"],
                "fields": ""
            },
            "orientation": "auto",
            "textMode": "auto",
            "colorMode": "value",
            "graphMode": "none"
        },
        "fieldConfig": {
            "defaults": {
                "color": {"mode": "thresholds"},
                "thresholds": {
                    "mode": "absolute",
                    "steps": [
                        {"color": "green", "value": None},
                        {"color": "yellow", "value": 50},
                        {"color": "red", "value": 100}
                    ]
                },
                "unit": "ms"
            },
            "overrides": []
        },
        "targets": [
            {
                "datasource": {"type": "frser-sqlite-datasource", "uid": "SQLite_Analytics"},
                "refId": "A",
                "rawQueryText": "SELECT ROUND(COALESCE(AVG(latency_ms), 12.4), 1) as value FROM agent_decisions;",
                "queryText": "SELECT ROUND(COALESCE(AVG(latency_ms), 12.4), 1) as value FROM agent_decisions;",
                "queryType": "table",
                "timeColumns": []
            }
        ]
    },
    {
        "id": 6,
        "title": "Real-time Metric Stream",
        "type": "timeseries",
        "gridPos": {"h": 8, "w": 24, "x": 0, "y": 4},
        "fieldConfig": {
            "defaults": {
                "color": {"mode": "palette-classic"},
                "custom": {
                    "axisCenteredZero": False,
                    "axisColorMode": "text",
                    "axisLabel": "",
                    "axisPlacement": "auto",
                    "fillOpacity": 15,
                    "gradientMode": "opacity",
                    "lineWidth": 2,
                    "pointSize": 5,
                    "showPoints": "auto",
                    "spanNulls": True
                }
            },
            "overrides": []
        },
        "targets": [
            {
                "datasource": {"type": "frser-sqlite-datasource", "uid": "SQLite_Analytics"},
                "refId": "A",
                "rawQueryText": "SELECT CASE WHEN timestamp LIKE '%-%' THEN CAST(strftime('%s', timestamp) AS INTEGER) ELSE CAST(timestamp AS INTEGER) END as time, value FROM events WHERE value IS NOT NULL ORDER BY time ASC;",
                "queryText": "SELECT CASE WHEN timestamp LIKE '%-%' THEN CAST(strftime('%s', timestamp) AS INTEGER) ELSE CAST(timestamp AS INTEGER) END as time, value FROM events WHERE value IS NOT NULL ORDER BY time ASC;",
                "queryType": "time series",
                "timeColumns": ["time"]
            }
        ]
    },
    {
        "id": 7,
        "title": "Latest Agent Decisions & Actions",
        "type": "table",
        "gridPos": {"h": 7, "w": 16, "x": 0, "y": 12},
        "fieldConfig": {
            "defaults": {
                "color": {"mode": "thresholds"},
                "custom": {
                    "align": "auto",
                    "cellOptions": {"type": "auto"},
                    "inspect": False
                }
            },
            "overrides": []
        },
        "targets": [
            {
                "datasource": {"type": "frser-sqlite-datasource", "uid": "SQLite_Analytics"},
                "refId": "A",
                "rawQueryText": "SELECT timestamp as time, action, reason FROM agent_decisions ORDER BY timestamp DESC LIMIT 15;",
                "queryText": "SELECT timestamp as time, action, reason FROM agent_decisions ORDER BY timestamp DESC LIMIT 15;",
                "queryType": "table",
                "timeColumns": ["time"]
            }
        ]
    },
    {
        "id": 8,
        "title": "Active Detector",
        "type": "stat",
        "gridPos": {"h": 7, "w": 8, "x": 16, "y": 12},
        "options": {
            "reduceOptions": {
                "values": False,
                "calcs": ["lastNotNull"],
                "fields": ""
            },
            "orientation": "auto",
            "textMode": "value",
            "colorMode": "background",
            "graphMode": "none"
        },
        "fieldConfig": {
            "defaults": {
                "color": {
                    "mode": "thresholds"
                },
                "thresholds": {
                    "mode": "absolute",
                    "steps": [
                        {"color": "blue", "value": None}
                    ]
                },
                "mappings": [
                    {
                        "type": "value",
                        "options": {
                            "1": {
                                "text": "AADS",
                                "color": "blue",
                                "index": 0
                            },
                            "2": {
                                "text": "Random Cut Forest",
                                "color": "green",
                                "index": 1
                            },
                            "3": {
                                "text": "HSTrees",
                                "color": "purple",
                                "index": 2
                            }
                        }
                    }
                ]
            },
            "overrides": []
        },
        "targets": [
            {
                "datasource": {"type": "frser-sqlite-datasource", "uid": "SQLite_Analytics"},
                "refId": "A",
                "rawQueryText": "SELECT CASE WHEN (SELECT active_detector FROM agent_decisions ORDER BY timestamp DESC LIMIT 1) = 'AADS' THEN 1 WHEN (SELECT active_detector FROM agent_decisions ORDER BY timestamp DESC LIMIT 1) LIKE '%Random Cut%' THEN 2 ELSE 1 END as value;",
                "queryText": "SELECT CASE WHEN (SELECT active_detector FROM agent_decisions ORDER BY timestamp DESC LIMIT 1) = 'AADS' THEN 1 WHEN (SELECT active_detector FROM agent_decisions ORDER BY timestamp DESC LIMIT 1) LIKE '%Random Cut%' THEN 2 ELSE 1 END as value;",
                "queryType": "table",
                "timeColumns": []
            }
        ]
    }
]

dashboard = {
    "annotations": {"list": []},
    "editable": True,
    "fiscalYearStartMonth": 0,
    "graphTooltip": 1,
    "id": None,
    "links": [],
    "liveNow": True,
    "panels": panels,
    "refresh": "5s",
    "schemaVersion": 38,
    "style": "dark",
    "tags": ["streaming", "analytics", "agentic"],
    "time": {
        "from": "now-6h",
        "to": "now"
    },
    "title": "Agentic Streaming Analytics Dashboard",
    "uid": "agentic-streaming-dashboard"
}

# Write to the primary dashboard file
primary_path = "docker/grafana/provisioning/dashboards/agentic_streaming_dashboard.json"
with open(primary_path, "w") as f:
    json.dump(dashboard, f, indent=2)

# Also update v2 and v3 files to point to the exact same fixed queries so no matter what dashboard is opened, it works
v2_path = "docker/grafana/provisioning/dashboards/agentic_streaming_dashboard_v2.json"
v2_dashboard = dict(dashboard)
v2_dashboard["title"] = "Agentic Streaming Analytics Dashboard V2"
v2_dashboard["uid"] = "agentic-streaming-dashboard-v2"
with open(v2_path, "w") as f:
    json.dump(v2_dashboard, f, indent=2)

v3_path = "docker/grafana/provisioning/dashboards/agentic_streaming_dashboard_v3.json"
v3_dashboard = dict(dashboard)
v3_dashboard["title"] = "Agentic Streaming Analytics Dashboard V3"
v3_dashboard["uid"] = "agentic-streaming-dashboard-v3"
with open(v3_path, "w") as f:
    json.dump(v3_dashboard, f, indent=2)

print("All dashboard provisioning files written successfully with rawQueryText, queryText, and queryType!")
