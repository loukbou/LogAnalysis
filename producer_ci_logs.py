import os
import json
import time
from kafka import KafkaProducer

ROOT_DIR = "./data/"
TOPIC = "ci_raw_lines"
BOOTSTRAP = "localhost:9094"

CHECKPOINT_FILE = "checkpoints.json"
DELAY_MS = 30
MAX_LINES_PER_FILE = 0   # 0 = send all

WATCH_MODE = False
WATCH_SLEEP_SECONDS = 10


def iter_log_files(root_dir: str):
    for day_folder in sorted(os.listdir(root_dir)):
        day_path = os.path.join(root_dir, day_folder)
        if not (os.path.isdir(day_path) and day_folder.startswith("log-")):
            continue
        for fname in sorted(os.listdir(day_path)):
            if fname.lower().endswith(".txt"):
                yield day_folder, os.path.join(day_path, fname)


def safe_read_lines(path: str):
    with open(path, "r", encoding="utf-8", errors="replace") as f:
        for i, line in enumerate(f, start=1):
            yield i, line.rstrip("\n")


def load_checkpoints():
    if not os.path.exists(CHECKPOINT_FILE):
        return {}
    try:
        with open(CHECKPOINT_FILE, "r") as f:
            return json.load(f)
    except Exception:
        return {}


def save_checkpoints(cp):
    tmp = CHECKPOINT_FILE + ".tmp"
    with open(tmp, "w") as f:
        json.dump(cp, f, indent=2)
    os.replace(tmp, CHECKPOINT_FILE)


def main_once(producer, checkpoints):
    sent = 0
    delay_s = DELAY_MS / 1000.0

    for day_folder, fpath in iter_log_files(ROOT_DIR):
        filename = os.path.basename(fpath)
        file_id = f"{day_folder}/{filename}"
        file_size = os.path.getsize(fpath)

        last_line_sent = int(checkpoints.get(file_id, {}).get("last_line_sent", 0))

        # ---- FILE START (once)
        if file_id not in checkpoints:
            producer.send(
                TOPIC,
                key=file_id.encode(),
                value={
                    "event_type": "file_start",
                    "file_id": file_id,
                    "day_folder": day_folder,
                    "filename": filename,
                    "path": fpath,
                    "ts_ingest": time.time(),
                },
            )
            producer.flush()

        current_line = last_line_sent

        # ---- RAW LINES
        for line_no, raw in safe_read_lines(fpath):
            if MAX_LINES_PER_FILE and line_no > MAX_LINES_PER_FILE:
                break
            if line_no <= last_line_sent:
                continue

            producer.send(
                TOPIC,
                key=file_id.encode(),
                value={
                    "event_type": "raw_line",
                    "file_id": file_id,
                    "day_folder": day_folder,
                    "filename": filename,
                    "line_no": line_no,
                    "message": raw,
                    "ts_ingest": time.time(),
                },
            )

            sent += 1
            current_line = line_no

            checkpoints[file_id] = {
                "last_line_sent": current_line,
                "file_size": file_size,
                "updated_at": time.time(),
            }

            if delay_s > 0:
                time.sleep(delay_s)

        # ---- FILE PROGRESS (optional)
        if current_line > last_line_sent:
            producer.send(
                TOPIC,
                key=file_id.encode(),
                value={
                    "event_type": "file_progress",
                    "file_id": file_id,
                    "day_folder": day_folder,
                    "filename": filename,
                    "last_line_sent": current_line,
                    "ts_ingest": time.time(),
                },
            )
            producer.flush()
            save_checkpoints(checkpoints)

        # ---- FILE END (exactly once)
        producer.send(
            TOPIC,
            key=file_id.encode(),
            value={
                "event_type": "file_end",
                "file_id": file_id,
                "day_folder": day_folder,
                "filename": filename,
                "path": fpath,
                "lines_sent": current_line,
                "ts_ingest": time.time(),
            },
        )
        producer.flush()

    return sent


def main():
    checkpoints = load_checkpoints()

    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks="all",
        linger_ms=20,
        retries=5,
    )

    try:
        if WATCH_MODE:
            while True:
                main_once(producer, checkpoints)
                time.sleep(WATCH_SLEEP_SECONDS)
        else:
            main_once(producer, checkpoints)
    finally:
        save_checkpoints(checkpoints)
        producer.close()


if __name__ == "__main__":
    main()
