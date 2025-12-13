import os
import json
import time
from kafka import KafkaProducer

# =========================
# CONFIG (inside code)
# =========================
ROOT_DIR = "./data/"                  # folder containing log-YYYY-MM-DD/
TOPIC = "ci_raw_lines"
BOOTSTRAP = "localhost:9094"

CHECKPOINT_FILE = "checkpoints.json"   # stores last_line_sent per file
DELAY_MS = 30                        
MAX_LINES_PER_FILE = 10                 # 0 means send all

WATCH_MODE = False                     # True = keep scanning folders forever
WATCH_SLEEP_SECONDS = 10               # how often to rescan for new lines/files


def iter_log_files(root_dir: str):
    for day_folder in sorted(os.listdir(root_dir)):
        day_path = os.path.join(root_dir, day_folder)
        if not (os.path.isdir(day_path) and day_folder.startswith("log-")):
            continue
        for fname in sorted(os.listdir(day_path)):
            if not fname.lower().endswith(".txt"):
                continue
            fpath = os.path.join(day_path, fname)
            if os.path.isfile(fpath):
                yield day_folder, fpath


def safe_read_lines(path: str):
    with open(path, "r", encoding="utf-8", errors="replace") as f:
        for i, line in enumerate(f, start=1):
            yield i, line.rstrip("\n")


def load_checkpoints():
    if not os.path.exists(CHECKPOINT_FILE):
        return {}
    try:
        with open(CHECKPOINT_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        # if file is corrupted, start fresh
        return {}


def save_checkpoints(cp):
    tmp = CHECKPOINT_FILE + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(cp, f, ensure_ascii=False, indent=2)
    os.replace(tmp, CHECKPOINT_FILE)


def main_once(producer: KafkaProducer, checkpoints: dict):
    sent = 0
    delay_s = DELAY_MS / 1000.0

    for day_folder, fpath in iter_log_files(ROOT_DIR):
        filename = os.path.basename(fpath)
        file_id = f"{day_folder}/{filename}"
        file_size = os.path.getsize(fpath)

        # how far we already sent in this file
        last_line_sent = int(checkpoints.get(file_id, {}).get("last_line_sent", 0))

        # emit file_start once per file (first time we ever see it)
        if file_id not in checkpoints:
            producer.send(
                TOPIC,
                key=file_id.encode("utf-8"),
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

        for line_no, raw in safe_read_lines(fpath):
            if MAX_LINES_PER_FILE and line_no > MAX_LINES_PER_FILE:
                break
            if line_no <= last_line_sent:
                continue

            producer.send(
                TOPIC,
                key=file_id.encode("utf-8"),
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

            # update checkpoint progressively (so crashes don’t replay too much)
            checkpoints[file_id] = {
                "last_line_sent": current_line,
                "file_size": file_size,
                "updated_at": time.time(),
            }

            if delay_s > 0:
                time.sleep(delay_s)

        # If we sent new lines this run, emit a file_progress marker
        if current_line > last_line_sent:
            producer.send(
                TOPIC,
                key=file_id.encode("utf-8"),
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

    producer.flush()
    return sent


def main():
    checkpoints = load_checkpoints()

    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
        acks="all",
        linger_ms=20,
        retries=5,
    )

    total = 0
    try:
        if WATCH_MODE:
            while True:
                total += main_once(producer, checkpoints)
                time.sleep(WATCH_SLEEP_SECONDS)
        else:
            total += main_once(producer, checkpoints)
    finally:
        save_checkpoints(checkpoints)
        producer.flush()
        producer.close()

    print(f"Done. Sent {total} new raw lines to topic '{TOPIC}'.")


if __name__ == "__main__":
    main()
