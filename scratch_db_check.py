import sqlite3
import pandas as pd
conn = sqlite3.connect('data/streaming_analytics.db')

print("detections:", pd.read_sql_query("SELECT COUNT(*) FROM detections WHERE prediction = 1", conn))
print("agent_decisions:", pd.read_sql_query("SELECT timestamp, action, reason FROM agent_decisions ORDER BY timestamp DESC LIMIT 5", conn))
print("events count:", pd.read_sql_query("SELECT COUNT(*) FROM events", conn))
