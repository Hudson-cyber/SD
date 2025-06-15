# src/tracker/tracker_server.py
"""
Servidor Tracker do sistema MiniBit
Responsável por descoberta de peers e distribuição inicial de blocos
"""
import socket
import threading
import json
import time
import random
import logging
from typing import Dict, List, Set, Optional, Tuple
from dataclasses import dataclass, asdict

from ..common.config import *
from ..common.messages import Message, MessageType, MessageBuilder

@dataclass
class PeerInfo:
    peer_id: str
    host: str
    port: int
    file_hash: str
    last_heartbeat: float
    initial_blocks: List[int]
    
    def is_alive(self, timeout: int = HEARTBEAT_INTERVAL * 2) -> bool:
        return time.time() - self.last_heartbeat < timeout

class TrackerServer:
    """Servidor central para descoberta de peers"""
    
    def __init__(self, host: str = TRACKER_HOST, port: int = TRACKER_PORT):
        self.host = host
        self.port = port
        self.peers: Dict[str, PeerInfo] = {}
        self.file_peers: Dict[str, Set[str]] = {}  # file_hash -> set of peer_ids
        self.running = False
        self.socket = None
        self.logger = self._setup_logger()
        
    def _setup_logger(self) -> logging.Logger:
        """Configura sistema de logs"""
        logger = logging.getLogger(f"Tracker-{self.port}")
        logger.setLevel(getattr(logging, LOG_LEVEL))
        
        if not logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter(LOG_FORMAT)
            handler.setFormatter(formatter)
            logger.addHandler(handler)
            
        return logger
    
    def start(self):
        """Inicia o servidor tracker"""
        try:
            self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            self.socket.bind((self.host, self.port))
            self.socket.listen(MAX_CONNECTIONS)
            
            self.running = True
            self.logger.info(f"Tracker iniciado em {self.host}:{self.port}")
            
            # Thread para limpeza de peers inativos
            cleanup_thread = threading.Thread(target=self._cleanup_inactive_peers)
            cleanup_thread.daemon = True
            cleanup_thread.start()
            
            # Loop principal para aceitar conexões
            while self.running:
                try:
                    client_socket, address = self.socket.accept()
                    client_thread = threading.Thread(
                        target=self._handle_client,
                        args=(client_socket, address)
                    )
                    client_thread.daemon = True
                    client_thread.start()
                    
                except Exception as e:
                    if self.running:
                        self.logger.error(f"Erro ao aceitar conexão: {e}")
                        
        except Exception as e:
            self.logger.error(f"Erro ao iniciar tracker: {e}")
        finally:
            self.stop()
    
    def _handle_client(self, client_socket: socket.socket, address: Tuple[str, int]):
        """Processa requisições de um cliente"""
        try:
            with client_socket:
                # Recebe mensagem
                data = client_socket.recv(4096).decode('utf-8')
                if not data:
                    return
                
                try:
                    message = Message.from_json(data)
                    response = self._process_message(message)
                    
                    if response:
                        client_socket.send(response.to_json().encode('utf-8'))
                        
                except json.JSONDecodeError:
                    self.logger.error(f"Mensagem JSON inválida de {address}")
                    
        except Exception as e:
            self.logger.error(f"Erro ao processar cliente {address}: {e}")
    
    def _process_message(self, message: Message) -> Optional[Message]:
        """Processa uma mensagem recebida"""
        try:
            if message.type == MessageType.REGISTER_PEER.value:
                return self._handle_register_peer(message)
            elif message.type == MessageType.GET_PEERS.value:
                return self._handle_get_peers(message)
            elif message.type == MessageType.REMOVE_PEER.value:
                return self._handle_remove_peer(message)
            elif message.type == MessageType.HEARTBEAT.value:
                return self._handle_heartbeat(message)
            else:
                self.logger.warning(f"Tipo de mensagem desconhecido: {message.type}")
                
        except Exception as e:
            self.logger.error(f"Erro ao processar mensagem: {e}")
            
        return None
    
    def _handle_register_peer(self, message: Message) -> Message:
        """Registra um novo peer"""
        peer_id = message.peer_id
        host = message.data.get('host')
        port = message.data.get('port')
        file_hash = message.data.get('file_hash', 'default')
        
        # Gera blocos iniciais aleatórios para o peer
        total_blocks = message.data.get('total_blocks', 10)  # padrão 10 blocos
        initial_blocks = self._generate_initial_blocks(total_blocks)
        
        # Registra o peer
        peer_info = PeerInfo(
            peer_id=peer_id,
            host=host,
            port=port,
            file_hash=file_hash,
            last_heartbeat=time.time(),
            initial_blocks=initial_blocks
        )
        
        self.peers[peer_id] = peer_info
        
        # Adiciona à lista de peers do arquivo
        if file_hash not in self.file_peers:
            self.file_peers[file_hash] = set()
        self.file_peers[file_hash].add(peer_id)
        
        self.logger.info(f"Peer {peer_id} registrado com blocos iniciais: {initial_blocks}")
        
        # Resposta com os blocos iniciais
        return Message(
            type="register_response",
            peer_id="tracker",
            timestamp=time.time(),
            data={
                'success': True,
                'initial_blocks': initial_blocks,
                'message': f'Peer {peer_id} registrado com sucesso'
            }
        )
    
    def _handle_get_peers(self, message: Message) -> Message:
        """Retorna lista de peers disponíveis"""
        requesting_peer = message.peer_id
        file_hash = message.data.get('file_hash', 'default')
        max_peers = min(message.data.get('max_peers', TRACKER_MAX_PEERS_RETURNED), 
                       TRACKER_MAX_PEERS_RETURNED)
        
        # Obtém peers ativos para o arquivo
        available_peers = []
        if file_hash in self.file_peers:
            for peer_id in self.file_peers[file_hash]:
                if peer_id != requesting_peer and peer_id in self.peers:
                    peer_info = self.peers[peer_id]
                    if peer_info.is_alive():
                        available_peers.append({
                            'peer_id': peer_id,
                            'host': peer_info.host,
                            'port': peer_info.port
                        })
        
        # Seleciona aleatoriamente até max_peers
        if len(available_peers) > max_peers:
            available_peers = random.sample(available_peers, max_peers)
        
        self.logger.info(f"Enviando {len(available_peers)} peers para {requesting_peer}")
        
        return Message(
            type="get_peers_response",
            peer_id="tracker",
            timestamp=time.time(),
            data={
                'peers': available_peers,
                'total_peers': len(self.file_peers.get(file_hash, set()))
            }
        )
    
    def _handle_remove_peer(self, message: Message) -> Message:
        """Remove um peer do sistema"""
        peer_id = message.peer_id
        
        if peer_id in self.peers:
            peer_info = self.peers[peer_id]
            file_hash = peer_info.file_hash
            
            # Remove das estruturas
            del self.peers[peer_id]
            if file_hash in self.file_peers:
                self.file_peers[file_hash].discard(peer_id)
                if not self.file_peers[file_hash]:
                    del self.file_peers[file_hash]
            
            self.logger.info(f"Peer {peer_id} removido do sistema")
            
            return Message(
                type="remove_response",
                peer_id="tracker",
                timestamp=time.time(),
                data={'success': True, 'message': f'Peer {peer_id} removido'}
            )
        
        return Message(
            type="remove_response", 
            peer_id="tracker",
            timestamp=time.time(),
            data={'success': False, 'message': f'Peer {peer_id} não encontrado'}
        )
    
    def _handle_heartbeat(self, message: Message) -> Message:
        """Atualiza heartbeat de um peer"""
        peer_id = message.peer_id
        
        if peer_id in self.peers:
            self.peers[peer_id].last_heartbeat = time.time()
            
            return Message(
                type="heartbeat_response",
                peer_id="tracker",
                timestamp=time.time(),
                data={'success': True}
            )
        
        return Message(
            type="heartbeat_response",
            peer_id="tracker", 
            timestamp=time.time(),
            data={'success': False, 'message': 'Peer não registrado'}
        )
    
    def _generate_initial_blocks(self, total_blocks: int) -> List[int]:
        """
        Gera subconjunto aleatório de blocos para um novo peer
        
        Args:
            total_blocks: Número total de blocos do arquivo
            
        Returns:
            Lista de IDs de blocos iniciais
        """
        # Gera entre 20% e 50% dos blocos aleatoriamente
        min_blocks = max(1, total_blocks // 5)  # 20%
        max_blocks = max(min_blocks, total_blocks // 2)  # 50%
        
        num_blocks = random.randint(min_blocks, max_blocks)
        return random.sample(range(total_blocks), num_blocks)
    
    def _cleanup_inactive_peers(self):
        """Remove peers inativos periodicamente"""
        while self.running:
            try:
                time.sleep(HEARTBEAT_INTERVAL)
                
                inactive_peers = []
                for peer_id, peer_info in self.peers.items():
                    if not peer_info.is_alive():
                        inactive_peers.append(peer_id)
                
                for peer_id in inactive_peers:
                    peer_info = self.peers[peer_id]
                    file_hash = peer_info.file_hash
                    
                    del self.peers[peer_id]
                    if file_hash in self.file_peers:
                        self.file_peers[file_hash].discard(peer_id)
                        if not self.file_peers[file_hash]:
                            del self.file_peers[file_hash]
                    
                    self.logger.info(f"Peer inativo removido: {peer_id}")
                    
            except Exception as e:
                self.logger.error(f"Erro na limpeza de peers: {e}")
    
    def get_stats(self) -> Dict:
        """Retorna estatísticas do tracker"""
        return {
            'total_peers': len(self.peers),
            'active_peers': sum(1 for p in self.peers.values() if p.is_alive()),
            'files_tracked': len(self.file_peers),
            'uptime': time.time() if hasattr(self, 'start_time') else 0
        }
    
    def stop(self):
        """Para o servidor tracker"""
        self.running = False
        if self.socket:
            self.socket.close()
        self.logger.info("Tracker parado")