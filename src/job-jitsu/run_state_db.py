# Creates a tiny SQLite DB to track job runs with ID and status.
# One file, always ready, no servers needed!
import sqlite3

with sqlite3.connect("job_runs.db") as db:
    db.execute("CREATE TABLE IF NOT EXISTS runs (id INTEGER PRIMARY KEY, status TEXT)")
    db.execute("INSERT INTO runs (status) VALUES (?)", ("completed",))
    db.commit()
    print("DB setup and sample run logged!")
