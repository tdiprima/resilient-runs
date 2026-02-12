# Resilient Runs
Retries + DB state + logging = resilient execution.

`unkillable.py`

This script is a resilience wrapper for long-running or critical automation tasks. It executes a job with structured logging, automatic retries, runtime tracking, and persistent state stored in a lightweight database so runs can be monitored and recovered safely. It also handles safe file writes and temporary workspace management to prevent corruption or partial output. In short, it turns fragile one-off scripts into fault-tolerant, restartable, production-ready automation that keeps running even when things try to break it.
