# Super simple JobResult class - counts processed jobs, failures, and runtime seconds.
# Ditch messy dicts forever!
from dataclasses import dataclass


@dataclass
class JobResult:
    processed: int
    failed: int
    duration_sec: float
