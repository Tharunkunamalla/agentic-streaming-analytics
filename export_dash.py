import urllib.request
import base64
import json

url = "http://localhost:3000/api/dashboards/uid/agentic-streaming-dashboard-v2"
req = urllib.request.Request(url)
base64string = base64.b64encode(b'admin:admin').decode('utf-8')
req.add_header("Authorization", "Basic %s" % base64string)

try:
    with urllib.request.urlopen(req) as response:
        data = json.loads(response.read().decode())
        with open("d:\\Streaming-project\\dashboard_export.json", "w") as f:
            json.dump(data, f, indent=2)
        print("Exported.")
except Exception as e:
    print(e)
