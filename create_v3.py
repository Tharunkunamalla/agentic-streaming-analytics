import json
import os

old_path = "d:\\Streaming-project\\docker\\grafana\\provisioning\\dashboards\\agentic_streaming_dashboard_v2.json"
new_path = "d:\\Streaming-project\\docker\\grafana\\provisioning\\dashboards\\agentic_streaming_dashboard_v3.json"

with open(old_path, "r") as f:
    data = json.load(f)

data["uid"] = "agentic-streaming-dashboard-v3"
data["title"] = "Agentic Streaming Analytics Dashboard V3"

for panel in data.get("panels", []):
    for target in panel.get("targets", []):
        if "rawSql" in target:
            target["queryText"] = target.pop("rawSql")
            
with open(new_path, "w") as f:
    json.dump(data, f, indent=2)

print("Created V3 dashboard with queryText.")
