"""
RESILIENT AUTOMATION TEMPLATE

A template for building unkillable automation scripts that handle:
- Unstable external resources (APIs, files, network)
- Persistent state across runs
- Safe cleanup and error handling
- Scheduled recurring execution

CUSTOMIZE THIS TEMPLATE:
1. Modify the database schema for your state tracking needs
2. Replace fetch_resource() with your actual data source
3. Add your business logic in the job() function
4. Adjust retry policies and scheduling intervals
5. Configure logging handlers (file, email, Sentry, etc.)
"""

# ============================================================================
# IMPORTS - Add your dependencies here
# ============================================================================
import logging.config
import sched
import sqlite3
import tempfile
import time
from dataclasses import dataclass
from pathlib import Path

from tenacity import retry, stop_after_attempt, wait_fixed

# TODO: Add your imports here (requests, pandas, boto3, etc.)


# ============================================================================
# CONFIGURATION - Customize these values
# ============================================================================

# Database configuration
STATE_DB = "automation_state.db"  # TODO: Change DB name for your use case

# Scheduling configuration
FIRST_RUN_DELAY_SEC = 5  # How long to wait before first run
RUN_INTERVAL_SEC = 10  # How often to run the job
MAX_RUNS = 3  # Set to None for infinite runs, or a number for testing

# Retry configuration
MAX_RETRY_ATTEMPTS = 3  # How many times to retry failed operations
RETRY_WAIT_SEC = 2  # Seconds to wait between retries

# Processing configuration
BATCH_SIZE = 10  # TODO: Number of items to process per run

# Output configuration
OUTPUT_DIR = Path("output")  # TODO: Customize output directory
REPORT_FILENAME = "summary.txt"  # TODO: Customize report name


# ============================================================================
# LOGGING SETUP - Configure structured logging
# ============================================================================
# TODO: Add file handler, email handler, or external logging service
logging.config.dictConfig(
    {
        "version": 1,
        "disable_existing_loggers": False,
        "handlers": {
            "console": {
                "class": "logging.StreamHandler",
                "level": "INFO",
                "formatter": "default",
            },
            # TODO: Add file handler for persistent logs
            # "file": {
            #     "class": "logging.FileHandler",
            #     "filename": "automation.log",
            #     "level": "DEBUG",
            #     "formatter": "detailed",
            # },
        },
        "formatters": {
            "default": {"format": "%(asctime)s - %(levelname)s - %(message)s"},
            # TODO: Add detailed formatter for debugging
            # "detailed": {
            #     "format": "%(asctime)s - %(name)s - %(levelname)s - "
            #               "%(funcName)s:%(lineno)d - %(message)s"
            # },
        },
        "root": {"handlers": ["console"], "level": "INFO"},
    }
)
logger = logging.getLogger(__name__)


# ============================================================================
# DATABASE SETUP - Persistent state tracking
# ============================================================================
# TODO: Modify schema to track your specific state (last_run_time, last_id, etc.)
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
        -- TODO: Add columns for your use case:
        -- , timestamp TEXT
        -- , last_processed_id INTEGER
        -- , error_message TEXT
    )
"""
)
conn.commit()
logger.info(f"SQLite state DB initialized: {STATE_DB}")


# ============================================================================
# DATA MODELS - Define your result structures
# ============================================================================
# TODO: Add fields relevant to your job results
@dataclass
class JobResult:
    """Results from a single job run."""
    processed: int
    failed: int
    duration_sec: float
    # TODO: Add your fields:
    # total_size_bytes: int = 0
    # api_calls_made: int = 0
    # records_updated: int = 0


# ============================================================================
# RESILIENT OPERATIONS - Functions with retry logic
# ============================================================================
# TODO: Replace this with your actual data fetching/processing function
@retry(stop=stop_after_attempt(MAX_RETRY_ATTEMPTS), wait=wait_fixed(RETRY_WAIT_SEC))
def fetch_resource(resource_id: int) -> str:
    """
    Fetch a single resource with automatic retries.

    TODO: Replace with your actual implementation:
    - API calls (requests.get, boto3 operations)
    - File downloads
    - Database queries
    - External service calls
    """
    # Demo: Simulate occasional failures
    import random
    if random.random() < 0.3:  # nosec B311
        raise ValueError(f"Fetch failed for resource {resource_id}")
    return f"data_chunk_{resource_id}"


# TODO: Add more resilient functions here
# @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
# def process_data(data: str) -> dict:
#     """Process raw data with exponential backoff retry."""
#     pass
#
# @retry(stop=stop_after_attempt(5), wait=wait_fixed(1))
# def send_notification(message: str) -> None:
#     """Send notification with retries."""
#     pass


# ============================================================================
# SCHEDULING SETUP
# ============================================================================
run_count = 0
scheduler = sched.scheduler(time.time, time.sleep)


# ============================================================================
# MAIN JOB FUNCTION - Your core automation logic
# ============================================================================
def job():
    """
    The main automation job that runs on schedule.

    TODO: Customize this function with your automation logic:
    1. Fetch data from your sources
    2. Process/transform the data
    3. Write results to destination
    4. Send notifications if needed
    5. Update state tracking
    """
    global run_count

    # Stop after MAX_RUNS (remove this check for infinite runs)
    if MAX_RUNS is not None and run_count >= MAX_RUNS:
        logger.info(f"Max runs ({MAX_RUNS}) reached. Exiting scheduler.")
        return

    run_count += 1
    logger.info(f"Starting job run #{run_count}")

    start_time = time.time()
    processed = 0
    failed = 0

    # ========================================================================
    # STEP 1: FETCH & PROCESS DATA (in safe temp directory)
    # ========================================================================
    # TODO: Customize the temporary file handling for your needs
    with tempfile.TemporaryDirectory() as tmpdir:
        temp_path = Path(tmpdir) / "processed_data.txt"
        logger.debug(f"Using temp dir: {tmpdir}")

        # TODO: Replace this loop with your data processing logic
        # Examples:
        # - Fetch from API and process JSON
        # - Download files and transform them
        # - Query database and aggregate results
        # - Scrape web pages and extract data
        for i in range(BATCH_SIZE):
            try:
                # TODO: Replace fetch_resource with your function
                data = fetch_resource(i)

                # TODO: Add your processing logic here
                # processed_data = transform(data)
                # validated_data = validate(processed_data)

                # Write to temp file (automatically cleaned up)
                with temp_path.open("a") as f:
                    f.write(data + "\n")
                processed += 1
                logger.debug(f"Processed item {i}")
            except Exception as e:
                logger.warning(f"Failed to process item {i} after retries: {e}")
                failed += 1

        # TODO: Add post-processing steps
        # - Aggregate results from temp files
        # - Generate summaries or statistics
        # - Prepare data for upload/export

    # ========================================================================
    # STEP 2: CALCULATE & LOG RESULTS
    # ========================================================================
    duration_sec = time.time() - start_time
    result = JobResult(processed, failed, duration_sec)

    logger.info(
        f"Job #{run_count} completed: processed={result.processed}, "
        f"failed={result.failed}, duration={result.duration_sec:.2f}s"
    )

    # ========================================================================
    # STEP 3: WRITE OUTPUT FILES
    # ========================================================================
    # TODO: Customize output format (CSV, JSON, PDF, etc.)
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    report_file = OUTPUT_DIR / REPORT_FILENAME

    # TODO: Generate your custom report format
    report_content = (
        f"Run #{run_count}\n"
        f"Processed: {result.processed}\n"
        f"Failed: {result.failed}\n"
        f"Duration: {result.duration_sec:.2f}s\n"
        # TODO: Add more report sections:
        # f"Total Size: {result.total_size_bytes} bytes\n"
        # f"Success Rate: {result.processed/(result.processed+result.failed)*100:.1f}%\n"
    )
    report_file.write_text(report_content, encoding="utf-8")
    logger.info(f"Report written to {report_file}")

    # ========================================================================
    # STEP 4: SAVE STATE TO DATABASE
    # ========================================================================
    # TODO: Customize what state you persist
    cur.execute(
        "INSERT INTO runs (status, processed, failed, duration_sec) "
        "VALUES (?, ?, ?, ?)",
        ("completed", result.processed, result.failed, result.duration_sec),
    )
    conn.commit()
    logger.info(f"State saved to {STATE_DB}")

    # ========================================================================
    # STEP 5: NOTIFICATIONS (Optional)
    # ========================================================================
    # TODO: Add notification logic
    # if result.failed > result.processed * 0.5:
    #     send_alert_email(f"High failure rate: {result.failed} failures")
    #
    # if run_count % 10 == 0:
    #     send_summary_notification(get_stats_from_db())

    # ========================================================================
    # STEP 6: SCHEDULE NEXT RUN
    # ========================================================================
    scheduler.enter(RUN_INTERVAL_SEC, 1, job)
    logger.info(f"Job completed. Next run in {RUN_INTERVAL_SEC} seconds.")


# ============================================================================
# STARTUP & SHUTDOWN
# ============================================================================
if __name__ == "__main__":
    # TODO: Add startup checks
    # - Verify credentials/API keys
    # - Check network connectivity
    # - Validate configuration
    # - Load previous state from DB if resuming

    logger.info("Automation script started. First job in {FIRST_RUN_DELAY_SEC} seconds...")
    scheduler.enter(FIRST_RUN_DELAY_SEC, 1, job)

    try:
        scheduler.run()
    except KeyboardInterrupt:
        logger.info("Received interrupt signal. Shutting down gracefully...")
    finally:
        # Cleanup on exit
        conn.close()
        logger.info(
            f"Script shutdown complete. Check '{OUTPUT_DIR}' and '{STATE_DB}' for results."
        )

        # TODO: Add final cleanup tasks
        # - Upload final reports to S3
        # - Send shutdown notification
        # - Archive old logs
