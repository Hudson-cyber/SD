from collections import defaultdict
import time
import random
import heapq
from threading import Lock

class RarestFirst:
    @staticmethod
    def select_blocks(peer, count=5):
        """Seleciona os N blocos mais raros que o peer não possui"""
        block_counts = defaultdict(int)
        available_peers = defaultdict(list)
        
        # Contar blocos e mapear peers que os possuem
        for peer_id, info in peer.known_peers.items():
            for block in info["blocks"]:
                block_counts[block] += 1
                available_peers[block].append(peer_id)
        
        missing = [b for b in block_counts if b not in peer.blocks]
        if not missing:
            return None, None
        
        # Pegar os N mais raros
        rarest = heapq.nsmallest(count, missing, key=lambda x: block_counts[x])
        
        # Mapear peers que possuem esses blocos
        peer_map = {block: available_peers[block] for block in rarest}
        
        return rarest, peer_map

class TitForTat:
    def __init__(self, optimistic_interval=30):
        self.unchoked = set()  # Peers desbloqueados
        self.last_optimistic = time.time()
        self.optimistic_interval = optimistic_interval
        self.upload_rates = defaultdict(int)  # Taxa de upload por peer
        self.download_rates = defaultdict(int)  # Taxa de download por peer
        self.last_unchoke_update = time.time()
        self.lock = Lock()
        self.optimistic_peer = None
        self.unchoke_history = defaultdict(list)  # Histórico de unchokes
    
    def update_upload_rate(self, peer_id, bytes_uploaded):
        with self.lock:
            self.upload_rates[peer_id] = bytes_uploaded
    
    def update_download_rate(self, peer_id, bytes_downloaded):
        with self.lock:
            self.download_rates[peer_id] = bytes_downloaded
    
    def update_unchoked(self, peer):
        current_time = time.time()
        
        # Atualizar a cada 10 segundos (exceto optimistic)
        if current_time - self.last_unchoke_update > 10:
            self._update_regular_unchoke(peer)
            self.last_unchoke_update = current_time
        
        # Atualizar optimistic unchoke periodicamente
        if current_time - self.last_optimistic > self.optimistic_interval:
            self._update_optimistic_unchoke(peer)
            self.last_optimistic = current_time
        
        # Retornar lista de unchoked peers (4 regulares + 1 otimista)
        with self.lock:
            return list(self.unchoked) + ([self.optimistic_peer] if self.optimistic_peer else [])
    
    def _update_regular_unchoke(self, peer):
        with self.lock:
            # Seleciona os 4 peers com maior taxa de download para nós
            sorted_peers = sorted(
                self.download_rates.items(),
                key=lambda x: x[1],
                reverse=True
            )
            
            # Pega os top 4 que possuem blocos que precisamos
            new_unchoked = set()
            for peer_id, _ in sorted_peers[:4]:
                if peer_id in peer.known_peers:
                    # Verifica se o peer tem blocos que nos interessam
                    peer_blocks = set(peer.known_peers[peer_id]["blocks"])
                    needed_blocks = set(peer.get_needed_blocks())
                    if peer_blocks & needed_blocks:
                        new_unchoked.add(peer_id)
            
            self.unchoked = new_unchoked
    
    def _update_optimistic_unchoke(self, peer):
        with self.lock:
            # Escolhe um peer que nunca foi unchoked ou há muito tempo
            all_peers = set(peer.known_peers.keys())
            tried_peers = set(self.unchoke_history.keys())
            candidates = list(all_peers - tried_peers)
            
            if not candidates:
                # Escolhe o peer menos recentemente unchoked
                candidates = list(all_peers)
                candidates.sort(key=lambda p: min(self.unchoke_history.get(p, [0])))
            
            if candidates:
                self.optimistic_peer = random.choice(candidates)
                self.unchoke_history[self.optimistic_peer].append(time.time())