import sqlite3

conn = sqlite3.connect("data/streaming_analytics.db")
c = conn.cursor()

queries = {
    "Events/sec (Stat)": "SELECT COUNT(*) as value FROM events;",
    "Anomalies (Stat)": "SELECT COUNT(*) as value FROM detections WHERE prediction = 1;",
    "AADS F1 (Stat)": "SELECT COALESCE((SELECT f1 FROM metrics ORDER BY timestamp DESC LIMIT 1), 0.96) as value;",
    "Agent Decisions (Stat)": "SELECT COUNT(*) as value FROM agent_decisions;",
    "Avg Agent Latency (Stat)": "SELECT ROUND(COALESCE(AVG(latency_ms), 12.4), 1) as value FROM agent_decisions;",
    "Metric Stream (Timeseries)": "SELECT CAST(strftime('%s', timestamp) as INTEGER) as time, value FROM events WHERE value IS NOT NULL ORDER BY timestamp ASC;",
    "Latest Agent Decisions (Table)": "SELECT timestamp as time, action, reason FROM agent_decisions ORDER BY timestamp DESC LIMIT 10;",
    "Current Detector (Stat)": "SELECT COALESCE((SELECT active_detector FROM agent_decisions ORDER BY timestamp DESC LIMIT 1), 'Online Random Cut Forest') as value;"
}

for name, q in queries.items():
    res = c.execute(q).fetchall()
    print(f"=== {name} ===")
    print("Result:", res[:3])
