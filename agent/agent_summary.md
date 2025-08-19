# Log Agent (SQLite Outbox → MongoDB)

A lightweight Python agent that:
- **Reads logs** from a file continuously  
- **Stores them first in a local SQLite outbox** (`outbox.db`)  
- **Pushes them in batches to MongoDB**  
- **Retries automatically** if MongoDB is down  

This ensures **no logs are lost** even if the network or MongoDB is unavailable.

---

## 🔧 How It Works

1. **Tail log file** (`LOG_FILE`) in real-time  
2. **Parse each line** → assign severity (`INFO`, `WARNING`, `ERROR`, etc.)  
3. **Persist into SQLite outbox** (`outbox.db`)  
4. **Flush unsent logs** to MongoDB in batches (`BATCH_SIZE`)  
5. **Retry thread** runs every `RETRY_INTERVAL` seconds to send pending logs  

---

## 📂 Log Structure

Each log is stored with the following fields:

| Field        | Type   | Description |
|--------------|--------|-------------|
| `event_id`   | UUID   | Unique identifier for the log |
| `timestamp`  | String | UTC time when captured |
| `message`    | Text   | Raw log line |
| `severity`   | Text   | INFO / WARNING / ERROR / CRITICAL |
| `sent`       | Int    | (SQLite only) `0=unsent`, `1=sent` |

## **Example MongoDB document:**
```json
{
  "event_id": "9a1c6a2d-11c4-4fbb-9e19-6f9d2a21f19d",
  "timestamp": "2025-08-20T03:10:45.123456",
  "message": "ERROR: something went wrong",
  "severity": "ERROR",
  "captured_at": "2025-08-20T03:10:45.567890"
}

## 🖥️ Console Output

When logs are read and pushed:

Monitoring /tmp/test.log for logs...
[+] Persisted log to outbox: event_id=7e2..., message="Hello world"
[✓] Sent log batch of size 1 to MongoDB
    └─ event_id=7e2..., message="Hello world"
[+] Persisted log to outbox: event_id=9a1..., message="ERROR: something went wrong"
[✓] Sent log batch of size 1 to MongoDB
    └─ event_id=9a1..., message="ERROR: something went wrong"


If MongoDB is unavailable:

[+] Persisted log to outbox: event_id=3c4..., message="ERROR: db down"
[x] Unexpected send error: <connection refused>
[↻] Retrying unsent logs...
