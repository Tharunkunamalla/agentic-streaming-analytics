import sqlite3
import time
import os
import sys
from pathlib import Path

DB_PATH = Path(__file__).resolve().parent.parent / "data" / "streaming_analytics.db"

def clear_screen():
    os.system('cls' if os.name == 'nt' else 'clear')

def main():
    print("Starting Live AI Decision Viewer...")
    if not DB_PATH.exists():
        print(f"Waiting for database at {DB_PATH}...")
        
    while True:
        try:
            if not DB_PATH.exists():
                time.sleep(2)
                continue
                
            conn = sqlite3.connect(str(DB_PATH))
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            
            cursor.execute('''
                SELECT timestamp, action, reason 
                FROM agent_decisions 
                ORDER BY timestamp DESC 
                LIMIT 5
            ''')
            rows = cursor.fetchall()
            
            clear_screen()
            print("=======================================================================================")
            print("                           LIVE AI AGENT DECISIONS (SMART DETECTIVE)                   ")
            print("=======================================================================================\n")
            
            if not rows:
                print("Waiting for the AI Agent to detect anomalies...\n")
                print("Tip: Run `python scripts/inject_unusual_behavior.py` in another terminal!")
            else:
                for row in rows:
                    time_str = row['timestamp'][:19]
                    action = row['action']
                    reason = row['reason']
                    print(f"🕒 [{time_str}] ⚡ ACTION: {action}")
                    print(f"   💬 REASON: {reason}")
                    print("-" * 87)
                    
            print("\n(Press Ctrl+C to stop viewing)")
            
            conn.close()
            time.sleep(2)
            
        except sqlite3.Error as e:
            print(f"Database error: {e}")
            time.sleep(2)
        except KeyboardInterrupt:
            print("\nExiting Live Viewer.")
            sys.exit(0)

if __name__ == "__main__":
    main()
