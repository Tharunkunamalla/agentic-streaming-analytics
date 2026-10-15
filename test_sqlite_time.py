import sqlite3
conn = sqlite3.connect('data/streaming_analytics.db')
print(conn.execute("SELECT strftime('%s', '2026-10-09T18:03:50.128648+00:00')").fetchone())
