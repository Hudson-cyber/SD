import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import socket
import threading
import pickle
import random
import time
from config import TRACKER_HOST, TRACKER_PORT, TRACKER_TIMEOUT, MAX_UNCHOKED

active_peers = {}
peers_lock = threading.Lock()

def remove_inactive_peers():
    """Remove peers that haven't checked in recently."""
    while True:
        time.sleep(TRACKER_TIMEOUT / 2)
        with peers_lock:
            current_time = time.time()
            to_remove = [port for port, last_seen in active_peers.items() if current_time - last_seen > TRACKER_TIMEOUT]
            for port in to_remove:
                del active_peers[port]
                print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] Removed inactive peer {port}")

def handle_peer_connection(connection):
    try:
        peer_port = pickle.loads(connection.recv(1024))
        with peers_lock:
            active_peers[peer_port] = time.time()
        with peers_lock:
            other_peers = list(active_peers.keys() - {peer_port})
        num_return = min(MAX_UNCHOKED, len(other_peers))
        selected = random.sample(other_peers, num_return)
        connection.send(pickle.dumps(selected))
    except Exception:
        pass
    finally:
        connection.close()

def start_tracker():
    threading.Thread(target=remove_inactive_peers, daemon=True).start()
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_socket:
        server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server_socket.bind((TRACKER_HOST, TRACKER_PORT))
        server_socket.listen()
        print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] Tracker online at {TRACKER_HOST}:{TRACKER_PORT}")
        while True:
            connection, _ = server_socket.accept()
            threading.Thread(target=handle_peer_connection, args=(connection,), daemon=True).start()

if __name__ == "__main__":
    start_tracker()
