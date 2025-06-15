# Simular 5 peers
import subprocess
for i in range(5):
    subprocess.Popen(f"python peer/peer_node.py --id peer{i} --file data.txt", shell=True)