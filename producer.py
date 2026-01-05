import os
import json
import time
from kafka import KafkaProducer

# =========================
# CONFIG
# =========================
ROOT_DIR = "./data"
TOPIC = "ci_logs"
BOOTSTRAP = "localhost:9094"
CHECKPOINT_FILE = "checkpoints.json"
DELAY_SECONDS = 0.02

producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

# -------------------------
# Checkpoint helpers
# -------------------------
def load_checkpoints():
    if not os.path.exists(CHECKPOINT_FILE):
        return {}
    with open(CHECKPOINT_FILE, "r", encoding="utf-8") as f:
        return json.load(f)

def save_checkpoints(cp):
    tmp = CHECKPOINT_FILE + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(cp, f, indent=2)
    os.replace(tmp, CHECKPOINT_FILE)

# -------------------------
# Incremental replay
# -------------------------
def replay_logs_incremental():
    checkpoints = load_checkpoints()
    print("🚀 Incremental producer started")

    for root, _, files in os.walk(ROOT_DIR):
        for fname in sorted(files):
            if not fname.endswith(".txt"):
                continue

            path = os.path.join(root, fname)
            file_id = os.path.relpath(path, ROOT_DIR)

            last_sent = checkpoints.get(file_id, 0)
            current_line = last_sent

            with open(path, "r", encoding="utf-8", errors="ignore") as f:
                for line_no, line in enumerate(f, start=1):
                    if line_no <= last_sent:
                        continue

                    producer.send(
                        TOPIC,
                        {
                            "file_id": file_id,
                            "line_no": line_no,
                            "message": line.rstrip(),
                        },
                    )

                    current_line = line_no
                    time.sleep(DELAY_SECONDS)

            # persist progress
            if current_line > last_sent:
                checkpoints[file_id] = current_line
                save_checkpoints(checkpoints)
                producer.flush()

    print("✅ Incremental replay finished")

if __name__ == "__main__":
    replay_logs_incremental()
