import json

path = "d:\\Streaming-project\\docker\\grafana\\provisioning\\dashboards\\agentic_streaming_dashboard.json"
with open(path, "r") as f:
    data = json.load(f)

for panel in data.get("panels", []):
    for target in panel.get("targets", []):
        if "query" in target:
            target["rawSql"] = target.pop("query")
            
        # For the timeseries panel (id=6), we must alias the time column to 'time'
        if panel.get("id") == 6 and "rawSql" in target:
            # Change "SELECT timestamp, value ..." to "SELECT timestamp as time, value ..."
            target["rawSql"] = target["rawSql"].replace("SELECT timestamp,", "SELECT timestamp as time,")
            
with open(path, "w") as f:
    json.dump(data, f, indent=2)

print("Dashboard fixed.")
