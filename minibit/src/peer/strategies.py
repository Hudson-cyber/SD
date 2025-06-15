import random
import time
import logging
from typing import Dict, List, Set, Optional, Tuple
from collections import defaultdict, Counter
import heapq

logger = logging.getLogger(__name__)

class PeerStrategies:
    def __init__(self, peer_id: str, max_unchoked: int = 4):
        self.peer_id = peer_id
        self.max_unchoked = max_unchoked
        
        # Estado dos peers
        self.peer_blocks: Dict[str, Set[int]] = {}  # Blocos que cada peer possui
        self.peer_download_rates: Dict[str, float] = {}  # Taxa de download de cada peer
        self.peer_upload_rates: Dict[str, float] = {}  # Taxa de upload para cada peer
        self.peer_last_request: Dict[str, float] = {}  # Último request de cada peer
        
        # Peers unchoked e interessados
        self.unchoked_peers: Set[str] = set()
        self.interested_peers: Set[str] = set()
        self.optimistic_unchoke_peer: Optional[str] = None
        
        # Controle de tempo
        self.last_unchoke_update = time.time()
        self.last_optimistic_update = time.time()
        self.unchoke_interval = 10.0  # 10 segundos
        self.optimistic_interval = 30.0  # 30 segundos
        
        # Estatísticas para decisões
        self.block_requests: Dict[str, int] = defaultdict(int)  # Requests por peer
        self.successful_downloads: Dict[str, int] = defaultdict(int)  # Downloads bem-sucedidos
        self.failed_requests: Dict[str, int] = defaultdict(int)  # Requests falhados
        
        # Histórico para Tit-for-Tat
        self.download_history: Dict[str, List[Tuple[float, int]]] = defaultdict(list)  # (timestamp, bytes)
        self.upload_history: Dict[str, List[Tuple[float, int]]] = defaultdict(list)  # (timestamp, bytes)
        
    def update_peer_blocks(self, peer_id: str, blocks: Set[int]):
        """Atualiza blocos que um peer possui"""
        self.peer_blocks[peer_id] = blocks.copy()
        logger.debug(f"Peer {peer_id} possui {len(blocks)} blocos")
    
    def add_peer_block(self, peer_id: str, block_id: int):
        """Adiciona um bloco que o peer possui"""
        if peer_id not in self.peer_blocks:
            self.peer_blocks[peer_id] = set()
        self.peer_blocks[peer_id].add(block_id)
    
    def remove_peer(self, peer_id: str):
        """Remove peer de todas as estruturas"""
        self.peer_blocks.pop(peer_id, None)
        self.peer_download_rates.pop(peer_id, None)
        self.peer_upload_rates.pop(peer_id, None)
        self.peer_last_request.pop(peer_id, None)
        self.unchoked_peers.discard(peer_id)
        self.interested_peers.discard(peer_id)
        
        if self.optimistic_unchoke_peer == peer_id:
            self.optimistic_unchoke_peer = None
        
        self.block_requests.pop(peer_id, None)
        self.successful_downloads.pop(peer_id, None)
        self.failed_requests.pop(peer_id, None)
        self.download_history.pop(peer_id, None)
        self.upload_history.pop(peer_id, None)
    
    def set_peer_interested(self, peer_id: str, interested: bool):
        """Define se peer está interessado"""
        if interested:
            self.interested_peers.add(peer_id)
        else:
            self.interested_peers.discard(peer_id)
    
    def select_rarest_blocks(self, missing_blocks: List[int], max_blocks: int = 5) -> List[int]:
        """
        Seleciona blocos mais raros usando algoritmo Rarest First
        """
        if not missing_blocks:
            return []
        
        # Calcular raridade de cada bloco
        block_rarity = self._calculate_block_rarity(missing_blocks)
        
        # Ordenar por raridade (mais raro primeiro) e depois por ID
        sorted_blocks = sorted(missing_blocks, key=lambda b: (block_rarity.get(b, 0), b))
        
        # Retornar os mais raros, limitado por max_blocks
        selected = sorted_blocks[:max_blocks]
        
        logger.debug(f"Blocos mais raros selecionados: {selected}")
        return selected
    
    def select_download_peers(self, block_id: int, max_peers: int = 3) -> List[str]:
        """
        Seleciona melhores peers para baixar um bloco específico
        """
        # Peers que possuem o bloco
        candidate_peers = []
        for peer_id, blocks in self.peer_blocks.items():
            if block_id in blocks and peer_id in self.interested_peers:
                candidate_peers.append(peer_id)
        
        if not candidate_peers:
            return []
        
        # Ordenar por critérios (taxa de download, confiabilidade, etc.)
        scored_peers = []
        for peer_id in candidate_peers:
            score = self._calculate_peer_download_score(peer_id)
            scored_peers.append((score, peer_id))
        
        # Ordenar por score (maior primeiro)
        scored_peers.sort(reverse=True)
        
        # Retornar melhores peers
        selected_peers = [peer_id for _, peer_id in scored_peers[:max_peers]]
        
        logger.debug(f"Peers selecionados para bloco {block_id}: {selected_peers}")
        return selected_peers
    
    def update_unchoked_peers(self, connected_peers: List[str]) -> Tuple[Set[str], Set[str]]:
        """
        Atualiza peers unchoked usando algoritmo Tit-for-Tat
        Retorna (novos_unchoked, novos_choked)
        """
        current_time = time.time()
        
        # Verificar se é hora de atualizar
        if current_time - self.last_unchoke_update < self.unchoke_interval:
            return set(), set()
        
        self.last_unchoke_update = current_time
        
        # Peers interessados e conectados
        eligible_peers = [p for p in connected_peers if p in self.interested_peers]
        
        if not eligible_peers:
            return set(), set()
        
        # Calcular scores dos peers para Tit-for-Tat
        peer_scores = []
        for peer_id in eligible_peers:
            score = self._calculate_tit_for_tat_score(peer_id)
            peer_scores.append((score, peer_id))
        
        # Ordenar por score (maior primeiro)
        peer_scores.sort(reverse=True)
        
        # Selecionar top peers para unchoke
        new_unchoked = set()
        for i, (score, peer_id) in enumerate(peer_scores):
            if i < self.max_unchoked:
                new_unchoked.add(peer_id)
        
        # Determinar mudanças
        newly_unchoked = new_unchoked - self.unchoked_peers
        newly_choked = self.unchoked_peers - new_unchoked
        
        # Atualizar estado
        self.unchoked_peers = new_unchoked
        
        if newly_unchoked or newly_choked:
            logger.info(f"Unchoked peers atualizados: +{newly_unchoked}, -{newly_choked}")
        
        return newly_unchoked, newly_choked
    
    def select_optimistic_unchoke(self, connected_peers: List[str]) -> Optional[str]:
        """
        Seleciona peer para optimistic unchoke
        """
        current_time = time.time()
        
        # Verificar se é hora de atualizar
        if current_time - self.last_optimistic_update < self.optimistic_interval:
            return self.optimistic_unchoke_peer
        
        self.last_optimistic_update = current_time
        
        # Peers elegíveis (interessados, conectados, não já unchoked)
        eligible_peers = [
            p for p in connected_peers 
            if p in self.interested_peers and p not in self.unchoked_peers
        ]
        
        if not eligible_peers:
            self.optimistic_unchoke_peer = None
            return None
        
        # Selecionar aleatoriamente, mas com preferência por peers novos
        weights = []
        for peer_id in eligible_peers:
            # Dar mais peso para peers com menos histórico
            history_size = len(self.download_history.get(peer_id, []))
            weight = max(1, 10 - history_size)  # Peso entre 1 e 10
            weights.append(weight)
        
        # Seleção ponderada
        selected_peer = random.choices(eligible_peers, weights=weights)[0]
        
        if selected_peer != self.optimistic_unchoke_peer:
            logger.info(f"Novo optimistic unchoke: {selected_peer}")
            self.optimistic_unchoke_peer = selected_peer
        
        return self.optimistic_unchoke_peer
    
    def record_download(self, peer_id: str, block_id: int, bytes_downloaded: int, success: bool):
        """Registra download realizado"""
        current_time = time.time()
        
        if success:
            self.successful_downloads[peer_id] += 1
            self.download_history[peer_id].append((current_time, bytes_downloaded))
            
            # Manter apenas últimos 10 downloads
            if len(self.download_history[peer_id]) > 10:
                self.download_history[peer_id] = self.download_history[peer_id][-10:]
        else:
            self.failed_requests[peer_id] += 1
        
        # Atualizar taxa de download
        self._update_download_rate(peer_id)
    
    def record_upload(self, peer_id: str, block_id: int, bytes_uploaded: int):
        """Registra upload realizado"""
        current_time = time.time()
        
        self.upload_history[peer_id].append((current_time, bytes_uploaded))
        
        # Manter apenas últimos 10 uploads
        if len(self.upload_history[peer_id]) > 10:
            self.upload_history[peer_id] = self.upload_history[peer_id][-10:]
        
        # Atualizar taxa de upload
        self._update_upload_rate(peer_id)
    
    def should_request_from_peer(self, peer_id: str, block_id: int) -> bool:
        """
        Decide se deve fazer request para um peer específico
        """
        # Verificar se peer possui o bloco
        if peer_id not in self.peer_blocks or block_id not in self.peer_blocks[peer_id]:
            return False
        
        # Verificar se peer não está choked
        if peer_id not in self.unchoked_peers and peer_id != self.optimistic_unchoke_peer:
            return False
        
        # Verificar rate limiting
        current_time = time.time()
        last_request = self.peer_last_request.get(peer_id, 0)
        if current_time - last_request < 1.0:  # Máximo 1 request por segundo por peer
            return False
        
        # Verificar histórico de falhas
        total_requests = self.successful_downloads[peer_id] + self.failed_requests[peer_id]
        if total_requests > 5:
            success_rate = self.successful_downloads[peer_id] / total_requests
            if success_rate < 0.5:  # Taxa de sucesso muito baixa
                return False
        
        return True
    
    def _calculate_block_rarity(self, blocks: List[int]) -> Dict[int, int]:
        """Calcula quantos peers possuem cada bloco"""
        rarity = {}
        
        for block_id in blocks:
            count = 0
            for peer_blocks in self.peer_blocks.values():
                if block_id in peer_blocks:
                    count += 1
            rarity[block_id] = count
        
        return rarity
    
    def _calculate_peer_download_score(self, peer_id: str) -> float:
        """Calcula score de um peer para download"""
        score = 0.0
        
        # Taxa de download (peso 40%)
        download_rate = self.peer_download_rates.get(peer_id, 0)
        score += download_rate * 0.4
        
        # Taxa de sucesso (peso 30%)
        total_requests = self.successful_downloads[peer_id] + self.failed_requests[peer_id]
        if total_requests > 0:
            success_rate = self.successful_downloads[peer_id] / total_requests
            score += success_rate * 30
        
        # Disponibilidade de blocos (peso 20%)
        blocks_available = len(self.peer_blocks.get(peer_id, set()))
        score += blocks_available * 0.2
        
        # Fator aleatório para diversidade (peso 10%)
        score += random.random() * 10
        
        return score
    
    def _calculate_tit_for_tat_score(self, peer_id: str) -> float:
        """Calcula score Tit-for-Tat de um peer"""
        current_time = time.time()
        score = 0.0
        
        # Taxa de download recente (últimos 60 segundos)
        recent_downloads = [
            (ts, bytes_) for ts, bytes_ in self.download_history.get(peer_id, [])
            if current_time - ts <= 60
        ]
        
        if recent_downloads:
            total_bytes = sum(bytes_ for _, bytes_ in recent_downloads)
            time_span = max(1, current_time - min(ts for ts, _ in recent_downloads))
            download_rate = total_bytes / time_span
            score += download_rate
        
        # Bonificação por reciprocidade
        upload_bytes = sum(bytes_ for _, bytes_ in self.upload_history.get(peer_id, []))
        download_bytes = sum(bytes_ for _, bytes_ in self.download_history.get(peer_id, []))
        
        if upload_bytes > 0 and download_bytes > 0:
            reciprocity = min(upload_bytes, download_bytes) / max(upload_bytes, download_bytes)
            score += reciprocity * 1000  # Bonificação por reciprocidade
        
        # Penalizar peers que não contribuem
        if upload_bytes == 0 and download_bytes > 1000:
            score *= 0.1  # Penalidade severa para leechers
        
        return score
    
    def _update_download_rate(self, peer_id: str):
        """Atualiza taxa de download de um peer"""
        current_time = time.time()
        history = self.download_history.get(peer_id, [])
        
        if not history:
            self.peer_download_rates[peer_id] = 0.0
            return
        
        # Calcular taxa dos últimos 30 segundos
        recent_history = [(ts, bytes_) for ts, bytes_ in history if current_time - ts <= 30]
        
        if not recent_history:
            self.peer_download_rates[peer_id] = 0.0
            return
        
        total_bytes = sum(bytes_ for _, bytes_ in recent_history)
        time_span = max(1, current_time - min(ts for ts, _ in recent_history))
        
        self.peer_download_rates[peer_id] = total_bytes / time_span
    
    def _update_upload_rate(self, peer_id: str):
        """Atualiza taxa de upload para um peer"""
        current_time = time.time()
        history = self.upload_history.get(peer_id, [])
        
        if not history:
            self.peer_upload_rates[peer_id] = 0.0
            return
        
        # Calcular taxa dos últimos 30 segundos
        recent_history = [(ts, bytes_) for ts, bytes_ in history if current_time - ts <= 30]
        
        if not recent_history:
            self.peer_upload_rates[peer_id] = 0.0
            return
        
        total_bytes = sum(bytes_ for _, bytes_ in recent_history)
        time_span = max(1, current_time - min(ts for ts, _ in recent_history))
        
        self.peer_upload_rates[peer_id] = total_bytes / time_span
    
    def get_statistics(self) -> Dict:
        """Retorna estatísticas das estratégias"""
        return {
            'unchoked_peers': list(self.unchoked_peers),
            'optimistic_unchoke': self.optimistic_unchoke_peer,
            'interested_peers': len(self.interested_peers),
            'known_peers': len(self.peer_blocks),
            'download_rates': dict(self.peer_download_rates),
            'upload_rates': dict(self.peer_upload_rates),
            'successful_downloads': dict(self.successful_downloads),
            'failed_requests': dict(self.failed_requests)
        }