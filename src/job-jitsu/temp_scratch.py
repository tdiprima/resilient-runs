# Makes a temp folder, writes a file inside, auto-cleans up. Zero leftovers!
import tempfile
from pathlib import Path

with tempfile.TemporaryDirectory() as temp_folder:
    scratch_file = Path(temp_folder) / "temp_data.txt"
    scratch_file.write_text("Temporary work data here")
    print(f"Wrote to: {scratch_file}")
