import json
import os

old_path = "d:\\Streaming-project\\docker\\grafana\\provisioning\\dashboards\\agentic_streaming_dashboard.json"
new_path = "d:\\Streaming-project\\docker\\grafana\\provisioning\\dashboards\\agentic_streaming_dashboard_v2.json"

with open(old_path, "r") as f:
    data = json.load(f)

data["uid"] = "agentic-streaming-dashboard-v2"
data["title"] = "Agentic Streaming Analytics Dashboard V2"

with open(new_path, "w") as f:
    json.dump(data, f, indent=2)

print("Created V2 dashboard.")
