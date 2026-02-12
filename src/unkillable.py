"""
Simulates nightly file sync/clean/email: fetches unstable resources,
processes in temp dir, logs results, writes report, tracks runs in DB.
Runs 4 demo cycles (3 work + 1 final check), then exits cleanly.
Never assumes clean inputs, files, or timing.
"""

import logging.config
import random
import sched
import sqlite3
import tempfile
import time
from dataclasses import dataclass
from pathlib import Path

from tenacity import retry, stop_after_attempt, wait_fixed

# logging.config — Structured logs from the start
logging.config.dictConfig(
    {
        "version": 1,
        "disable_existing_loggers": False,
        "handlers": {
            "console": {
                "class": "logging.StreamHandler",
                "level": "INFO",
                "formatter": "default",  # Simple default formatter
            }
        },
        "formatters": {
            "default": {"format": "%(asctime)s - %(levelname)s - %(message)s"}
        },
        "root": {"handlers": ["console"], "level": "INFO"},
    }
)
logger = logging.getLogger(__name__)

# sqlite3 — Persistent state (one file, zero setup)
STATE_DB = "automation_state.db"
conn = sqlite3.connect(STATE_DB, check_same_thread=False)
cur = conn.cursor()
cur.execute(
    """
    CREATE TABLE IF NOT EXISTS runs (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        status TEXT,
        processed INTEGER,
        failed INTEGER,
        duration_sec REAL
    )
"""
)
conn.commit()
logger.info("SQLite state DB initialized")


# dataclasses — Structured results (no mysterious dicts)
@dataclass
class JobResult:
    processed: int
    failed: int
    duration_sec: float


# tenacity — Retries with policy (no silent failures)
@retry(stop=stop_after_attempt(3), wait=wait_fixed(2))
def fetch_resource(resource_id: int) -> str:
    """Simulate unstable external resource fetch (30% failure rate)."""
    if random.random() < 0.3:  # nosec B311
        raise ValueError(f"Fetch failed for resource {resource_id}")
    return f"data_chunk_{resource_id}"


# Global run counter for demo (stops after 3 real runs)
run_count = 0
MAX_RUNS = 3

# sched — Controlled scheduling (own your timing)
scheduler = sched.scheduler(time.time, time.sleep)


def job():
    """The core automation job: fetch/sync/clean/report."""
    global run_count

    # Stop after MAX_RUNS (demo finite execution)
    if run_count >= MAX_RUNS:
        logger.info("Max runs reached. Exiting scheduler.")
        return

    run_count += 1
    logger.info(f"Starting job run #{run_count}")

    start_time = time.time()
    processed = 0
    failed = 0

    # tempfile — Safe scratch space (no leaks)
    with tempfile.TemporaryDirectory() as tmpdir:
        temp_path = Path(tmpdir) / "processed_data.txt"
        logger.debug(f"Using temp dir: {tmpdir}")

        # Simulate syncing 10 resources (with retries)
        for i in range(10):
            try:
                data = fetch_resource(i)
                # Append to temp file (safe processing/cleanup)
                with temp_path.open("a") as f:
                    f.write(data + "\n")
                processed += 1
                logger.debug(f"Processed resource {i}")
            except Exception as e:
                logger.warning(f"Failed resource {i} after retries: {e}")
                failed += 1

    duration_sec = time.time() - start_time
    result = JobResult(processed, failed, duration_sec)

    # Log structured result
    logger.info(
        f"Job #{run_count} result: processed={result.processed}, "
        f"failed={result.failed}, duration={result.duration_sec:.2f}s"
    )

    # pathlib — Safe, readable filesystem ops
    output_dir = Path("output")
    output_dir.mkdir(parents=True, exist_ok=True)
    report_file = output_dir / "summary.txt"
    report_content = (
        f"Run #{run_count} - Processed: {result.processed}, "
        f"Failed: {result.failed}, Duration: {result.duration_sec:.2f}s\n"
    )
    report_file.write_text(report_content, encoding="utf-8")
    logger.info(f"Report written to {report_file}")

    # Save to state DB
    cur.execute(
        "INSERT INTO runs (status, processed, failed, duration_sec) "
        "VALUES (?, ?, ?, ?)",
        ("completed", result.processed, result.failed, result.duration_sec),
    )
    conn.commit()
    logger.info(f"State saved to {STATE_DB}")

    # Reschedule next run (every 10 seconds for demo)
    scheduler.enter(10, 1, job)
    logger.info("Job completed. Next run scheduled.")


# Kick off the scheduler: first run in 5 seconds
logger.info("Automation script started. First job in 5 seconds...")
scheduler.enter(5, 1, job)
scheduler.run()

# Cleanup on exit
conn.close()
logger.info(
    "Script completed successfully. Check 'output/summary.txt' and 'automation_state.db'."
)
