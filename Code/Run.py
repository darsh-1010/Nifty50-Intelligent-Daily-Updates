import subprocess
import os
 
# 🔹 SET YOUR MAIN FOLDER HERE
base_path = r"C:\Users\10102\Downloads\DAILY UPDATE 2"
 
# Build full paths to your scripts
script1 = os.path.join(base_path, "UPDATE_NEWS.py")
script2 = os.path.join(base_path, "updateohlcv.py")
script3 = os.path.join(base_path, "update_pcr.py")
 
# Optional: Set working directory so internal relative paths work
os.chdir(base_path)
 
# Run both scripts
subprocess.run(["python", script1])
subprocess.run(["python", script2])
subprocess.run(["python", script3])
 
 