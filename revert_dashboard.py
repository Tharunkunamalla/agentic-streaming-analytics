import json

path = "d:\\Streaming-project\\docker\\grafana\\provisioning\\dashboards\\agentic_streaming_dashboard.json"
with open(path, "r") as f:
    data = json.load(f)

for panel in data.get("panels", []):
    for target in panel.get("targets", []):
        if "rawSql" in target:
            # Revert alias if it was added
            target["rawSql"] = target["rawSql"].replace("SELECT timestamp as time,", "SELECT timestamp,")
            # Revert key
            target["query"] = target.pop("rawSql")
            
with open(path, "w") as f:
    json.dump(data, f, indent=2)

print("Dashboard reverted to original.")
