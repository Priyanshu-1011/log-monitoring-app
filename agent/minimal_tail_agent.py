#!/usr/bin/env python3
"""
minimal_tail_agent.py (with SQLite outbox)
- tails a file
- prints matches
- persists matches to outbox.db
"""

import os, time, re, argparse, traceback, sqlite3
from datetime import datetime

ROOT = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(ROOT, "outbox.db")

# --- DB init ---------------------------------------------------------------
def init_db(path):
    conn = sqlite3.connect(path, check_same_thread=False)
    cur = conn.cursor()
    cur.execute("""
    CREATE TABLE IF NOT EXISTS outbox (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        timestamp TEXT,
        filename TEXT,
        line TEXT,
        sent INTEGER DEFAULT 0
    )
    """)
    conn.commit()
    return conn

db_conn = init_db(DB_PATH)
db_cur = db_conn.cursor()

def persist_match(filename, line):
    ts = datetime.utcnow().isoformat() + "Z"
    # keep only first 4000 chars
    snippet = line[:4000]
    db_cur.execute("INSERT INTO outbox (timestamp, filename, line) VALUES (?, ?, ?)", (ts, filename, snippet))
    db_conn.commit()
    print(f"[PERSISTED {ts}] id={db_cur.lastrowid}")

# --- Tailer ----------------------------------------------------------------
def tail_file(path, pattern, start_at_end=True):
    pat = re.compile(pattern, re.IGNORECASE)
    print(f"[{datetime.utcnow().isoformat()}] DEBUG: Watching {path} pattern={pattern}")
    pos = 0
    inode = None
    while True:
        try:
            if not os.path.exists(path):
                time.sleep(0.5)
                continue
            st = os.stat(path)
            if inode != st.st_ino:
                f = open(path, "r", errors="ignore")
                inode = st.st_ino
                if start_at_end:
                    f.seek(0, os.SEEK_END)
                else:
                    f.seek(0, os.SEEK_SET)
                pos = f.tell()
                print(f"[{datetime.utcnow().isoformat()}] DEBUG: Opened {path} (inode={inode}) pos={pos}")
            else:
                f = open(path, "r", errors="ignore")
                f.seek(pos)

            while True:
                line = f.readline()
                if not line:
                    pos = f.tell()
                    f.close()
                    break
                line = line.rstrip("\n")
                if pat.search(line):
                    ts = datetime.utcnow().isoformat() + "Z"
                    print(f"[MATCH {ts}] {line}")
                    try:
                        persist_match(path, line)
                    except Exception as e:
                        print("Persist failed:", e)
        except KeyboardInterrupt:
            print("Stopping tailer (KeyboardInterrupt)")
            try:
                db_conn.close()
            except:
                pass
            return
        except Exception as e:
            print("Tailer inner error:", e)
            traceback.print_exc()
            time.sleep(1)

# --- CLI -------------------------------------------------------------------
if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("path", help="path to logfile to watch")
    ap.add_argument("--regex", default=r"(ERROR|CRITICAL|FATAL|Exception|Traceback)")
    ap.add_argument("--start-at-begin", dest="start_at_end", action="store_false")
    ap.add_argument("--start-at-end", dest="start_at_end", action="store_true", default=True)
    args = ap.parse_args()
    tail_file(args.path, args.regex, start_at_end=args.start_at_end)


# --- End of file -----------------------------tested---------------------------
# TO EXECUTE THIS SCRIPT:IN TERMINAL, RUN:
# PYTHONUNBUFFERED=1 python3 ~/Documents/python/py_proj/minimal_tail_agent.py /tmp/test.log --start-at-begin