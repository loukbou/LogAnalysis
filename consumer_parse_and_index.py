import json
import re
from datetime import datetime
from kafka import KafkaConsumer
from elasticsearch import Elasticsearch

TOPIC = "ci_logs"
BOOTSTRAP = "localhost:9094"
ES_HOST = "http://localhost:9200"
INDEX = "ci-logs"

print("🚀 Consumer starting")

consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=BOOTSTRAP,
    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    auto_offset_reset="earliest",
    enable_auto_commit=True,
)

es = Elasticsearch(ES_HOST)
print("🔌 Elasticsearch ping:", es.ping())

# in-memory state per file
state = {}

def now():
    return datetime.utcnow().isoformat()

def infer_context(builder: str):
    b = builder.lower()
    ctx = {}
    if "linux" in b or "ubuntu" in b:
        ctx["platform"] = "linux"
    elif "win" in b:
        ctx["platform"] = "windows"
    elif "mac" in b or "yosemite" in b:
        ctx["platform"] = "mac"

    if "debug" in b:
        ctx["build_type"] = "debug"
    elif "pgo" in b:
        ctx["build_type"] = "pgo"

    for t in [
        "xpcshell", "mochitest", "reftest", "web-platform-tests",
        "crashtest", "jsreftest", "cppunit", "gtest", "marionette"
    ]:
        if t in b:
            ctx["test_type"] = t
            break

    return ctx


for msg in consumer:
    e = msg.value
    fid = e["file_id"]
    line = e.get("message")
    if line is None:
        # Not a log line event → skip safely
        continue

    if fid not in state:
        state[fid] = {
            "file_id": fid,
            "builder": None,
            "slave": None,
            "starttime_epoch": None,
            "buildid": None,
            "builduid": None,
            "revision": None,
            "result_status": None,
            "result_code": None,

            "platform": None,
            "build_type": None,
            "test_type": None,

            "error_count": 0,
            "warning_count": 0,

            "cpu": {},
            "io": {},
            "sections": [],
            "current_section": None,
            "section_start": None,
        }

    job = state[fid]

    doc = {
        "@timestamp": now(),
        "file_id": fid,
        "line_no": e["line_no"],
        "message": line,
    }

    # ---------------- METADATA ----------------
    if line.startswith("builder:"):
        job["builder"] = line.split(":", 1)[1].strip()
        job.update(infer_context(job["builder"]))

    elif line.startswith("slave:"):
        job["slave"] = line.split(":", 1)[1].strip()

    elif line.startswith("starttime:"):
        try:
            job["starttime_epoch"] = float(line.split(":", 1)[1].strip())
        except:
            pass

    elif line.startswith("buildid:"):
        job["buildid"] = line.split(":", 1)[1].strip()

    elif line.startswith("builduid:"):
        job["builduid"] = line.split(":", 1)[1].strip()

    elif line.startswith("revision:"):
        job["revision"] = line.split(":", 1)[1].strip()

    elif line.startswith("results:"):
        m = re.search(r"(\w+)\s*\((\d+)\)", line)
        if m:
            job["result_status"] = m.group(1)
            job["result_code"] = int(m.group(2))

    # ---------------- SECTIONS ----------------
    if "Started" in line and "====" in line:
        job["current_section"] = line
        job["section_start"] = now()

    if "Finished" in line and "====" in line:
        job["sections"].append({
            "name": job["current_section"],
            "start_time": job["section_start"],
            "end_time": now(),
        })
        job["current_section"] = None
        job["section_start"] = None

    # ---------------- ERRORS / WARNINGS ----------------
    if re.search(r"\bERROR\b", line):
        job["error_count"] += 1
        doc["level"] = "ERROR"
    elif re.search(r"\bWARNING\b", line):
        job["warning_count"] += 1
        doc["level"] = "WARNING"
    else:
        doc["level"] = "INFO"

    # ---------------- CPU / IO ----------------
    m = re.search(r"CPU\s+(user|system|idle)[^\d]*([\d.]+)", line, re.I)
    if m:
        job["cpu"][m.group(1).lower()] = float(m.group(2))

    m = re.search(r"I/O\s+(read|write)\s+bytes.*?(\d+)", line, re.I)
    if m:
        job["io"][m.group(1).lower() + "_bytes"] = int(m.group(2))

    # ---------------- ATTACH JOB CONTEXT ----------------
    doc.update({
        "builder": job["builder"],
        "slave": job["slave"],
        "platform": job["platform"],
        "build_type": job["build_type"],
        "test_type": job["test_type"],
        "result_status": job["result_status"],
        "result_code": job["result_code"],
        "error_count": job["error_count"],
        "warning_count": job["warning_count"],
        "cpu": job["cpu"],
        "io": job["io"],
        "sections": job["sections"],
    })

    es.index(index=INDEX, document=doc)

    print("📥 indexed", fid, e["line_no"])
