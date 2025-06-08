import subprocess
import os
import logging

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s: %(message)s')

# Set your main folder here (updated to current workspace)
base_path = r"C:\Users\KUNJ\Downloads\New folder\Nifty50-Intelligent-Daily-Updates\Code"

# Build full paths to your scripts
script1 = os.path.join(base_path, "UPDATE_NEWS.py")
script2 = os.path.join(base_path, "updateohlcv.py")
script3 = os.path.join(base_path, "update_pcr.py")

# Optional: Set working directory so internal relative paths work
os.chdir(base_path)

# Run scripts with error handling
for script in [script1, script2, script3]:
    if not os.path.exists(script):
        logging.error(f"Script not found: {script}")
        continue
    logging.info(f"Running {script}")
    result = subprocess.run(["python", script])
    if result.returncode != 0:
        logging.error(f"Script failed: {script}")
    else:
        logging.info(f"Script completed: {script}")

 