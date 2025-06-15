# algorithms.py
import random
import time
from collections import Counter
import threading

UNCHECK_INTERVAL = 10
MAX_FIXED_UNCHOKES = 4

class PeerAlgorithms:
    def __init__(self, peer_id, blocks):
        self.peer_id = peer_id
        self.blocks = blocks
        self.peers_known = []
        self.unchoked_peers = set()
        self.optimistic_peer = None

    def update_peers(self, peers_known, current_blocks):
        self.peers_known = peers_known
        self.blocks = current_blocks

    def calcular_blocos_raros(self):
        todos_blocos = [bloco for peer in self.peers_known for bloco in peer["blocks"]]
        contagem = Counter(todos_blocos)
        blocos_necessarios = [b for b in contagem if b not in self.blocks]
        if blocos_necessarios:
            bloco_mais_raro = min(blocos_necessarios, key=lambda b: contagem[b])
            return bloco_mais_raro
        return None

    def tit_for_tat(self):
        def loop():
            while True:
                time.sleep(UNCHECK_INTERVAL)
                peer_scores = {}
                for peer in self.peers_known:
                    raros = [b for b in peer["blocks"] if b not in self.blocks]
                    peer_scores[peer["peer_id"]] = len(raros)
                top_peers = sorted(peer_scores, key=peer_scores.get, reverse=True)[:MAX_FIXED_UNCHOKES]
                self.unchoked_peers = set(top_peers)
                restantes = list(set(peer_scores.keys()) - self.unchoked_peers)
                self.optimistic_peer = random.choice(restantes) if restantes else None
                print(f"[{self.peer_id}] Unchoked: {self.unchoked_peers} | Optimistic: {self.optimistic_peer}")

        threading.Thread(target=loop, daemon=True).start()
