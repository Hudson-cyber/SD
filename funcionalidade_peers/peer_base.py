# peer_base.py
import socket
import threading
import json
import random

class PeerBase:
    def __init__(self, peer_id, host, port, all_blocks):
        self.peer_id = peer_id
        self.host = host
        self.port = port
        self.blocks = random.sample(all_blocks, k=random.randint(1, len(all_blocks)//2))
        self.peers_known = []
        self.storage = {block: f"data_{block}" for block in self.blocks}

    def register_with_tracker(self, tracker_host, tracker_port):
        msg = json.dumps({
            "action": "register",
            "peer_id": self.peer_id,
            "host": self.host,
            "port": self.port,
            "blocks": self.blocks
        })
        with socket.socket() as s:
            s.connect((tracker_host, tracker_port))
            s.sendall(msg.encode())
            resp = json.loads(s.recv(4096).decode())
            print(f"[{self.peer_id}] Registro: {resp}")

    def get_peers(self, tracker_host, tracker_port):
        msg = json.dumps({
            "action": "get_peers",
            "peer_id": self.peer_id
        })
        with socket.socket() as s:
            s.connect((tracker_host, tracker_port))
            s.sendall(msg.encode())
            peers = json.loads(s.recv(4096).decode())
            self.peers_known = peers
            print(f"[{self.peer_id}] Conhece peers: {[p['peer_id'] for p in peers]}")

    def start_server(self):
        def handle_client(conn):
            request = json.loads(conn.recv(4096).decode())
            bloco = request.get("block")
            data = self.storage.get(bloco)
            conn.sendall(json.dumps({"block": bloco, "data": data}).encode())
            conn.close()

        def server_thread():
            server = socket.socket()
            server.bind((self.host, self.port))
            server.listen(5)
            print(f"[{self.peer_id}] Aguardando conexões em {self.host}:{self.port}")
            while True:
                conn, addr = server.accept()
                threading.Thread(target=handle_client, args=(conn,)).start()

        threading.Thread(target=server_thread, daemon=True).start()
