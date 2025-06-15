import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import socket
import threading
import pickle
import random
import time
from collections import Counter
from config import FILE_NAME


from config import *
from utils.utils import assemble_file

class Peer:
    def __init__(self, peer_id, total_blocks, initial_blocks):
        self.peer_id = peer_id
        self.port = PEER_PORT_BASE + peer_id
        self.blocks = set(initial_blocks)
        self.total_blocks = total_blocks
        self.peers_known = set()
        self.allowed_peers = set()
        self.optimistic_peer = None
        self.blocked_by = set()
        self.running = True
        self.lock = threading.Lock()
        self.log_file = open(f'logs/peer-{self.peer_id}-{FILE_NAME}.log', 'w')

        os.makedirs('logs', exist_ok=True)
        self.log_file = open(f'logs/peer_{self.peer_id}.log', 'w')
        self.write_log(f'Started with {len(self.blocks)} blocks.')

    def write_log(self, message):
        timestamp = time.strftime('%Y-%m-%d %H:%M:%S')
        log_entry = f'[{timestamp}] [Peer {self.peer_id}] {message}'
        print(log_entry)
        self.log_file.write(log_entry + '\n')
        self.log_file.flush()

    def start_server(self):
        threading.Thread(target=self._server_loop, daemon=True).start()

    def _server_loop(self):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_socket:
            server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            server_socket.bind((TRACKER_HOST, self.port))
            server_socket.listen()
            while self.running:
                try:
                    conn, _ = server_socket.accept()
                    threading.Thread(target=self._handle_request, args=(conn,), daemon=True).start()
                except OSError:
                    break

    def _handle_request(self, connection):
        try:
            request = pickle.loads(connection.recv(4096))
            if request['type'] == 'list':
                connection.sendall(pickle.dumps(list(self.blocks)))
            elif request['type'] == 'block_request':
                requester_port = request['requester_port']
                block_id = request['block_id']
                with self.lock:
                    allowed = (requester_port in self.allowed_peers) or (requester_port == self.optimistic_peer)
                if allowed and block_id in self.blocks:
                    with open(f'files/blocks/{FILE_NAME}_block_{block_id}', 'rb') as f:
                        connection.sendall(pickle.dumps(f.read()))
                else:
                    connection.sendall(pickle.dumps(b'CHOKED'))
        except Exception:
            pass
        finally:
            connection.close()

    def _connect_peer(self, peer_port, message):
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                sock.settimeout(2)
                sock.connect((TRACKER_HOST, peer_port))
                sock.sendall(pickle.dumps(message))
                return pickle.loads(sock.recv(65536))
        except Exception:
            return None

    def contact_tracker(self):
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                sock.settimeout(5)
                sock.connect((TRACKER_HOST, TRACKER_PORT))
                sock.sendall(pickle.dumps(self.port))
                peers = pickle.loads(sock.recv(4096))
                with self.lock:
                    self.peers_known.update(peers)
        except Exception:
            self.write_log('Tracker is offline.')

    def _get_blocks_in_network(self):
        block_counts = Counter()
        peer_blocks = {}
        with self.lock:
            peers_copy = self.peers_known.copy()

        for peer_port in peers_copy:
            if peer_port == self.port:
                continue
            blocks = self._connect_peer(peer_port, {'type': 'list'})
            if blocks is not None:
                peer_blocks[peer_port] = set(blocks)
                block_counts.update(blocks)
            else:
                with self.lock:
                    self.peers_known.discard(peer_port)
        return block_counts, peer_blocks

    def _select_rarest_block(self, block_counts):
        needed = set(range(self.total_blocks)) - self.blocks
        if not needed:
            return None
        rarity = {b: block_counts[b] for b in needed if b in block_counts}
        if not rarity:
            return None
        min_count = min(rarity.values())
        candidates = [b for b, count in rarity.items() if count == min_count]
        return random.choice(candidates)

    def update_peers_loop(self):
        while self.running:
            time.sleep(UNCHOKE_INTERVAL)
            if not self.peers_known:
                continue
            _, peer_blocks = self._get_blocks_in_network()
            needed_blocks = set(range(self.total_blocks)) - self.blocks
            with self.lock:
                self.blocked_by.clear()
                if not needed_blocks:
                    selected = random.sample(list(self.peers_known - {self.port}), min(MAX_UNCHOKED, len(self.peers_known - {self.port})))
                    self.allowed_peers = set(selected)
                    self.optimistic_peer = None
                    continue
            scores = {}
            for port, has_blocks in peer_blocks.items():
                with self.lock:
                    if port in self.blocked_by:
                        scores[port] = -1
                        continue
                scores[port] = len(needed_blocks.intersection(has_blocks))
            ranked_peers = sorted(scores.keys(), key=lambda p: scores[p], reverse=True)
            new_allowed = set(ranked_peers[:MAX_UNCHOKED])
            choked_peers = [p for p in ranked_peers if p not in new_allowed]
            optimistic = random.choice(choked_peers) if choked_peers else None
            with self.lock:
                self.allowed_peers = new_allowed
                self.optimistic_peer = optimistic
            allowed_names = sorted([f'P{p-PEER_PORT_BASE}' for p in new_allowed])
            opt_name = f'P{optimistic-PEER_PORT_BASE}' if optimistic else "None"
            self.write_log(f"New unchoked peers: {allowed_names}, optimistic: {opt_name}")

    def download_blocks_loop(self):
        while len(self.blocks) < self.total_blocks:
            block_counts, peer_blocks = self._get_blocks_in_network()
            target = self._select_rarest_block(block_counts)
            if target is None:
                self.write_log("No needed blocks available.")
                time.sleep(5)
                continue
            with self.lock:
                candidates = list(self.allowed_peers)
                if self.optimistic_peer:
                    candidates.append(self.optimistic_peer)
            random.shuffle(candidates)
            downloaded = False
            for peer_port in candidates:
                if peer_port in peer_blocks and target in peer_blocks[peer_port]:
                    message = {'type': 'block_request', 'block_id': target, 'requester_port': self.port}
                    result = self._connect_peer(peer_port, message)
                    if result and result != b'CHOKED':
                        with open(f'files/blocks/{FILE_NAME}_block_{target}', 'wb') as f:
                            f.write(result)
                        with self.lock:
                            self.blocks.add(target)
                            percent = (100 * len(self.blocks)) // self.total_blocks
                        self.write_log(f"Downloaded block {target} from peer {peer_port - PEER_PORT_BASE} ({percent}%)")
                        downloaded = True
                        break
                    elif result == b'CHOKED':
                        self.write_log(f"Choked by peer {peer_port - PEER_PORT_BASE}")
                        with self.lock:
                            self.blocked_by.add(peer_port)
            if not downloaded:
                time.sleep(random.uniform(1.0, 2.0))
        output_path = f'files/downloads/peer-{self.peer_id}-{FILE_NAME}'
        assemble_file(sorted(list(self.blocks)), 'files/blocks', output_path, FILE_NAME)
        self.write_log(f'File saved to {output_path}. Now seeding...')

        while True:
            time.sleep(3600)

    def run(self):
        self.start_server()
        threading.Thread(target=self._tracker_loop, daemon=True).start()
        threading.Thread(target=self.update_peers_loop, daemon=True).start()
        self.download_blocks_loop()

    def _tracker_loop(self):
        while self.running:
            self.contact_tracker()
            time.sleep(TRACKER_UPDATE_INTERVAL)

if __name__ == '__main__':
    if len(sys.argv) < 4:
        print("How to use: python3 peer.py <id> <total_blocks> <block1> <block2> ...")
        sys.exit(1)
    peer_id = int(sys.argv[1])
    total_blocks = int(sys.argv[2])
    initial_blocks = list(map(int, sys.argv[3:]))
    peer = Peer(peer_id, total_blocks, initial_blocks)
    peer.run()
