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

state = {}

def now():
    return datetime.utcnow().isoformat()

def infer_context(builder: str):
    b = builder.lower()
    ctx = {
        "platform": None,
        "architecture": None,
        "build_type": None,
        "test_type": None,
    }

    if "linux" in b or "ubuntu" in b:
        ctx["platform"] = "linux"
    elif "win" in b:
        ctx["platform"] = "windows"
    elif "mac" in b or "yosemite" in b:
        ctx["platform"] = "mac"

    if "x64" in b or "64" in b:
        ctx["architecture"] = "64bit"
    elif "32" in b:
        ctx["architecture"] = "32bit"

    if "debug" in b:
        ctx["build_type"] = "debug"
    elif "pgo" in b:
        ctx["build_type"] = "pgo"

    for t in [
        "xpcshell", "mochitest", "reftest",
        "web-platform-tests", "crashtest",
        "jsreftest", "cppunit", "gtest", "marionette"
    ]:
        if t in b:
            ctx["test_type"] = t
            break

    return ctx

for msg in consumer:
    e = msg.value
    line = e.get("message")
    if line is None:
        continue

    fid = e["file_id"]

    if fid not in state:
        state[fid] = {
            "builder": None,
            "slave": None,
            "starttime_epoch": None,
            "buildid": None,
            "builduid": None,
            "revision": None,
            "result_status": None,
            "result_code": None,

            "platform": None,
            "architecture": None,
            "build_type": None,
            "test_type": None,

            "error_count": 0,
            "warning_count": 0,
            "has_errors": False,
            "has_warnings": False,

            "cpu": {},
            "io": {},
            "sections": [],
            "current_section": None,
            "section_start": None,
        }

    job = state[fid]

    # ---------- Metadata ----------
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

    # ---------- Sections ----------
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

    # ---------- Errors / warnings ----------
    level = "INFO"
    if re.search(r"\bERROR\b", line):
        job["error_count"] += 1
        job["has_errors"] = True
        level = "ERROR"
    elif re.search(r"\bWARNING\b", line):
        job["warning_count"] += 1
        job["has_warnings"] = True
        level = "WARNING"

    # ---------- CPU ----------
    m = re.search(r"CPU\s+(user|system|idle)[^\d]*([\d.]+)", line, re.I)
    if m:
        job["cpu"][m.group(1).lower()] = float(m.group(2))

    # ---------- IO ----------
    m = re.search(r"I/O\s+(read|write)\s+bytes.*?(\d+)", line, re.I)
    if m:
        job["io"][m.group(1).lower() + "_bytes"] = int(m.group(2))

    # ---------- elapsed time ----------
    elapsed_time = None
    m = re.search(r"elapsedTime=([\d.]+)", line)
    if m:
        elapsed_time = float(m.group(1))

    # ---------- Final document ----------
    doc = {
        "@timestamp": now(),
        "file_id": fid,
        "line_no": e.get("line_no"),
        "message": line,
        "level": level,

        "builder": job["builder"],
        "slave": job["slave"],
        "starttime_epoch": job["starttime_epoch"],
        "buildid": job["buildid"],
        "builduid": job["builduid"],
        "revision": job["revision"],
        "result_status": job["result_status"],
        "result_code": job["result_code"],

        "platform": job["platform"],
        "architecture": job["architecture"],
        "build_type": job["build_type"],
        "test_type": job["test_type"],

        "error_count": job["error_count"],
        "warning_count": job["warning_count"],
        "has_errors": job["has_errors"],
        "has_warnings": job["has_warnings"],

        "cpu": job["cpu"],
        "io": job["io"],
        "elapsed_time": elapsed_time,
        "sections": job["sections"],
    }

    es.index(index=INDEX, document=doc)
    print("📥 indexed", fid, e.get("line_no"))
