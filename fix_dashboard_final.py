import json

path = "d:\\Streaming-project\\docker\\grafana\\provisioning\\dashboards\\agentic_streaming_dashboard.json"
with open(path, "r") as f:
    data = json.load(f)

for panel in data.get("panels", []):
    for target in panel.get("targets", []):
        q = target.get("query", target.get("rawSql", ""))
        
        # Ensure we have the proper aliases
        if "COUNT(*) / 10.0 FROM events" in q:
            q = "SELECT COUNT(*) / 10.0 as value FROM events;"
        elif "COUNT(*) FROM detections" in q:
            q = "SELECT COUNT(*) as value FROM detections WHERE prediction = 1;"
        elif "f1 FROM metrics" in q:
            q = "SELECT f1 as value FROM metrics ORDER BY timestamp DESC LIMIT 1;"
        elif "COUNT(*) FROM agent_decisions" in q:
            q = "SELECT COUNT(*) as value FROM agent_decisions;"
        elif "AVG(latency_ms)" in q:
            q = "SELECT AVG(latency_ms) as value FROM agent_decisions;"
        elif "SELECT timestamp, value FROM events" in q or "SELECT strftime('%s', timestamp) as time, value FROM events" in q:
            q = "SELECT strftime('%s', timestamp) as time, value FROM events ORDER BY timestamp ASC;"
        elif "SELECT timestamp, action, reason FROM agent_decisions" in q or "SELECT timestamp as time, action, reason" in q:
            q = "SELECT timestamp as time, action, reason FROM agent_decisions ORDER BY timestamp DESC LIMIT 5;"
        elif "active_detector FROM agent_decisions" in q:
            q = "SELECT active_detector as value FROM agent_decisions ORDER BY timestamp DESC LIMIT 1;"
            
        target["rawSql"] = q
        if "query" in target:
            del target["query"]
            
with open(path, "w") as f:
    json.dump(data, f, indent=2)

print("Dashboard fixed completely.")
