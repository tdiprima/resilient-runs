# Retries a flaky function up to 3 times, waits 2 secs between tries.
# Handles failures like a boss - no more manual try/except hell!
import random

from tenacity import retry, stop_after_attempt, wait_fixed


@retry(stop=stop_after_attempt(3), wait=wait_fixed(2))
def get_data():
    # 60% chance of fake failure
    if random.random() < 0.6:  # nosec B311
        raise ValueError("Network hiccup!")
    return "Data fetched OK"


# Test it
print(get_data())
