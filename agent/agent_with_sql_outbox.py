#!/usr/bin/env python3
"""
agent_with_sql_outbox.py

- Tails a log file (LOG_FILE)
- Persists matching lines to a local SQLite outbox (outbox.db)
- Sends unsent rows in batches to central MongoDB
- Background retry thread flushes unsent logs every interval
- Thread-safe SQLite usage: each thread opens its own connection
- Console output kept in the exact format you requested
"""

import os
import time
import uuid
import sqlite3
import threading
from datetime import datetime
from pymongo import MongoClient, errors
from dotenv import load_dotenv

# ---------- Config ----------
load_dotenv()
LOG_FILE = os.getenv("LOG_FILE", "/tmp/test.log")
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017/")
DB_NAME = os.getenv("DB_NAME", "log_monitoring")
COLLECTION_NAME = os.getenv("COLLECTION_NAME", "logs")
OUTBOX_DB = os.getenv("OUTBOX_DB", "outbox.db")
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "5"))
RETRY_INTERVAL = int(os.getenv("RETRY_INTERVAL", "30"))  # seconds

# ---------- Helpers ----------
def now_iso():
    return datetime.utcnow().isoformat()

# ---------- SQLite helpers ----------
def init_sqlite(path=OUTBOX_DB):
    """Create (if needed) and return a new sqlite3.Connection for the caller thread."""
    conn = sqlite3.connect(path, timeout=30, isolation_level=None)  # autocommit mode (isolation_level=None)
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS outbox (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            event_id TEXT UNIQUE,
            timestamp TEXT,
            message TEXT,
            severity TEXT,
            sent INTEGER DEFAULT 0
        )
    """)
    conn.commit()
    return conn

def persist_log(conn, log_entry):
    cur = conn.cursor()
    try:
        cur.execute(
            "INSERT INTO outbox (event_id, timestamp, message, severity, sent) VALUES (?, ?, ?, ?, 0)",
            (log_entry["event_id"], log_entry["timestamp"], log_entry["message"], log_entry["severity"])
        )
        conn.commit()
        print(f'[+] Persisted log to outbox: event_id={log_entry["event_id"]}, message="{log_entry["message"]}"')
    except sqlite3.IntegrityError:
        # event_id already exists locally (rare)
        print(f'[!] Duplicate log skipped: event_id={log_entry["event_id"]}')

def fetch_unsent(conn, limit=BATCH_SIZE):
    cur = conn.cursor()
    cur.execute("SELECT event_id, timestamp, message, severity FROM outbox WHERE sent = 0 ORDER BY id ASC LIMIT ?", (limit,))
    return cur.fetchall()

def mark_sent(conn, event_ids):
    if not event_ids:
        return
    cur = conn.cursor()
    # Use parameter substitution safely
    qmarks = ",".join(["?"] * len(event_ids))
    cur.execute(f"UPDATE outbox SET sent = 1 WHERE event_id IN ({qmarks})", tuple(event_ids))
    conn.commit()

# ---------- Mongo helpers ----------
def init_mongo():
    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
    coll = client[DB_NAME][COLLECTION_NAME]
    try:
        coll.create_index("event_id", unique=True)
    except Exception:
        # ignore index creation errors (e.g., partial duplicates)
        pass
    return coll

def send_batch_to_mongo(coll, docs):
    """Attempt to insert_many docs (ordered=False). Return tuple(success_ids, failed_event_ids)."""
    if not docs:
        return [], []

    try:
        coll.insert_many(docs, ordered=False)
        # If successful, return all event_ids as succeeded
        return [d["event_id"] for d in docs], []
    except errors.BulkWriteError as bwe:
        # partial success possible
        # Determine which event_ids exist on server now and treat them as succeeded
        # Simpler: query central DB for the event_ids from this batch
        event_ids = [d["event_id"] for d in docs]
        found_cursor = coll.find({"event_id": {"$in": event_ids}}, {"event_id": 1})
        found = {doc["event_id"] for doc in found_cursor}
        succeeded = [eid for eid in event_ids if eid in found]
        failed = [eid for eid in event_ids if eid not in found]
        return succeeded, failed
    except Exception as e:
        # Other error: treat as total failure
        raise

# ---------- Core flush logic (each caller uses its own sqlite conn) ----------
def flush_outbox(conn, mongo_coll):
    rows = fetch_unsent(conn, BATCH_SIZE)
    if not rows:
        return

    # Build docs for Mongo
    docs = []
    event_ids = []
    for event_id, timestamp, message, severity in rows:
        docs.append({
            "event_id": event_id,
            "timestamp": timestamp,
            "message": message,
            "severity": severity,
            "captured_at": now_iso()
        })
        event_ids.append(event_id)

    try:
        succeeded, failed = send_batch_to_mongo(mongo_coll, docs)
        if succeeded:
            mark_sent(conn, succeeded)
            print(f"[✓] Sent log batch of size {len(succeeded)} to MongoDB")
            for eid in succeeded:
                # print message for each succeeded event (fetch from docs)
                m = next((d["message"] for d in docs if d["event_id"] == eid), "")
                print(f"    └─ event_id={eid}, message=\"{m}\"")
        if failed:
            # If some failed, print them and leave them for retry
            print(f"[x] {len(failed)} events failed to be confirmed; will retry later")
    except Exception as e:
        # Do not crash the caller thread; surface error for retry/backoff
        print(f"[x] Unexpected send error: {e}")
        raise

# ---------- Background retry thread ----------
def retry_loop(db_path=OUTBOX_DB, interval=RETRY_INTERVAL):
    # Each thread needs its own sqlite connection
    conn = init_sqlite(db_path)
    mongo_coll = init_mongo()
    while True:
        try:
            time.sleep(interval)
            print("[↻] Retrying unsent logs...")
            flush_outbox(conn, mongo_coll)
        except Exception as e:
            # Keep thread alive on errors
            print(f"[x] Retry loop error: {e}")
            # small sleep to avoid tight loop on persistent errors
            time.sleep(5)

# ---------- Log parsing & monitor ----------
def parse_log_line(line):
    line = line.rstrip("\n")
    if not line:
        return None
    severity = "INFO"
    up = line.upper()
    for level in ["CRITICAL", "FATAL", "ERROR", "WARNING", "WARN"]:
        if level in up:
            severity = level
            break
    return {
        "event_id": str(uuid.uuid4()),
        "timestamp": now_iso(),
        "message": line,
        "severity": severity
    }

def ensure_log_file(path):
    # create file if missing, ensure parent dir exists
    parent = os.path.dirname(path) or "."
    try:
        os.makedirs(parent, exist_ok=True)
    except Exception:
        pass
    if not os.path.exists(path):
        open(path, "a").close()

def follow(fileobj):
    fileobj.seek(0, os.SEEK_END)
    while True:
        line = fileobj.readline()
        if not line:
            time.sleep(0.5)
            continue
        yield line

def monitor_logs():
    # Main thread sqlite conn (only used by main thread functions)
    conn = init_sqlite(OUTBOX_DB)
    mongo_coll = init_mongo()

    # start retry thread (its own sqlite conn)
    t = threading.Thread(target=retry_loop, args=(OUTBOX_DB, RETRY_INTERVAL), daemon=True)
    t.start()

    ensure_log_file(LOG_FILE)
    print(f"Monitoring {LOG_FILE} for logs...")

    # open file in read mode and follow
    try:
        with open(LOG_FILE, "r") as f:
            for line in follow(f):
                entry = parse_log_line(line)
                if not entry:
                    continue
                # persist and immediately attempt flush
                persist_log(conn, entry)
                try:
                    flush_outbox(conn, mongo_coll)
                except Exception:
                    # if immediate flush failed, leave for retry thread
                    # no backoff here; retry thread will handle
                    pass
    except Exception as e:
        print(f"[Error] Monitor loop terminated: {e}")

if __name__ == "__main__":
    monitor_logs()
