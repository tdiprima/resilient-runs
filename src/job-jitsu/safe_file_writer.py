# Creates output folder and writes a report file using Path - works everywhere, no string hacks!
from pathlib import Path

output_folder = Path("reports")
output_folder.mkdir(parents=True, exist_ok=True)
summary_file = output_folder / "daily_summary.txt"
summary_file.write_text("All jobs done successfully!")
