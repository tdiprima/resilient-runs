"""
TODO: Describe what your automation does (e.g., "Fetch data from API, process, and email report")
"""

import logging.config
import sched
import sqlite3
import tempfile
import time
from dataclasses import dataclass
from pathlib import Path

from tenacity import retry, stop_after_attempt, wait_fixed

# ============================================================================
# LOGGING SETUP
# ============================================================================
logging.config.dictConfig({
    "version": 1,
    "disable_existing_loggers": False,
    "handlers": {
        "console": {
            "class": "logging.StreamHandler",
            "level": "INFO",
            "formatter": "default",
        }
    },
    "formatters": {
        "default": {"format": "%(asctime)s - %(levelname)s - %(message)s"}
    },
    "root": {"handlers": ["console"], "level": "INFO"},
})
logger = logging.getLogger(__name__)

# ============================================================================
# DATABASE SETUP (tracks run history)
# ============================================================================
STATE_DB = "automation_state.db"  # TODO: Change DB name if needed
conn = sqlite3.connect(STATE_DB, check_same_thread=False)
cur = conn.cursor()
cur.execute("""
    CREATE TABLE IF NOT EXISTS runs (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        status TEXT,
        processed INTEGER,
        failed INTEGER,
        duration_sec REAL
    )
""")
conn.commit()
logger.info("Database initialized")

# ============================================================================
# DATA STRUCTURES
# ============================================================================
@dataclass
class JobResult:
    processed: int
    failed: int
    duration_sec: float
    # TODO: Add any other fields you need (e.g., error_messages, records_updated)

# ============================================================================
# CORE FUNCTIONS
# ============================================================================

@retry(stop=stop_after_attempt(3), wait=wait_fixed(2))
def fetch_data(item_id: int) -> str:
    """
    TODO: Replace this with your actual data fetching logic
    (API call, file download, database query, etc.)

    HINT: This function automatically retries 3 times with 2-second waits
    """
    # Example: return requests.get(f"https://api.example.com/data/{item_id}").text
    raise NotImplementedError("Replace with your fetch logic")


def process_data(data: str) -> str:
    """
    TODO: Add your data processing logic here
    (parse, transform, validate, etc.)
    """
    # Example: return json.loads(data)
    return data


# ============================================================================
# MAIN JOB
# ============================================================================
scheduler = sched.scheduler(time.time, time.sleep)

def job():
    """The main automation job that runs on schedule."""
    logger.info("Starting job")
    start_time = time.time()
    processed = 0
    failed = 0

    # Use temp directory for safe file operations
    with tempfile.TemporaryDirectory() as tmpdir:
        temp_path = Path(tmpdir) / "working_data.txt"

        # TODO: Replace range(10) with your actual work items
        # Example: for item_id in get_pending_items():
        for item_id in range(10):
            try:
                # Fetch with automatic retries
                data = fetch_data(item_id)

                # Process the data
                result = process_data(data)

                # TODO: Do something with the result
                # Example: send_email(result) or save_to_db(result)
                with temp_path.open("a") as f:
                    f.write(str(result) + "\n")

                processed += 1
                logger.debug(f"Processed item {item_id}")

            except Exception as e:
                logger.warning(f"Failed item {item_id}: {e}")
                failed += 1

    duration_sec = time.time() - start_time
    result = JobResult(processed, failed, duration_sec)

    logger.info(f"Job complete: {processed} processed, {failed} failed, {duration_sec:.2f}s")

    # ========================================================================
    # SAVE RESULTS
    # ========================================================================

    # Write summary report
    output_dir = Path("output")  # TODO: Change output path if needed
    output_dir.mkdir(parents=True, exist_ok=True)
    report = output_dir / "summary.txt"
    report.write_text(
        f"Processed: {result.processed}, Failed: {result.failed}, "
        f"Duration: {result.duration_sec:.2f}s\n"
    )
    logger.info(f"Report saved to {report}")

    # Save to database
    cur.execute(
        "INSERT INTO runs (status, processed, failed, duration_sec) VALUES (?, ?, ?, ?)",
        ("completed", result.processed, result.failed, result.duration_sec),
    )
    conn.commit()

    # ========================================================================
    # RESCHEDULE
    # ========================================================================
    # TODO: Change interval (in seconds)
    interval = 3600  # Run every hour
    scheduler.enter(interval, 1, job)
    logger.info(f"Next run in {interval} seconds")


# ============================================================================
# START
# ============================================================================
if __name__ == "__main__":
    logger.info("Automation started")

    # TODO: Change initial delay (in seconds)
    initial_delay = 5

    scheduler.enter(initial_delay, 1, job)

    try:
        scheduler.run()
    except KeyboardInterrupt:
        logger.info("Stopping...")
    finally:
        conn.close()
        logger.info("Shutdown complete")
