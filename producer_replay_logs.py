import os
import json
import time
from kafka import KafkaProducer

ROOT_DIR = "./data"
TOPIC = "ci_logs"
BOOTSTRAP = "localhost:9094"
DELAY = 0.02

producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

def replay_logs():
    print("🚀 Producer started")

    for root, _, files in os.walk(ROOT_DIR):
        for fname in sorted(files):
            if not fname.endswith(".txt"):
                continue

            path = os.path.join(root, fname)
            file_id = os.path.relpath(path, ROOT_DIR)

            print(f"📤 Replaying {file_id}")

            with open(path, "r", encoding="utf-8", errors="ignore") as f:
                for line_no, line in enumerate(f, start=1):
                    producer.send(
                        TOPIC,
                        {
                            "file_id": file_id,
                            "line_no": line_no,
                            "message": line.rstrip(),
                        },
                    )
                    time.sleep(DELAY)

            producer.flush()

if __name__ == "__main__":
    replay_logs()
