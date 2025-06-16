import time
import threading
import logging
import requests
import json
import signal
import sys
from typing import List, Dict, Optional, Set
from pathlib import Path

from .block_manager import BlockManager
from .communication import PeerCommunication
from .strategies import PeerStrategies
from SD.minibit.src.common.messages import Message, MessageType
from SD.minibit.src.common.config import Config

logger = logging.getLogger(__name__)

class PeerNode:
    def __init__(self, peer_id: str, port: int, tracker_url: str, file_path: Optional[str] = None):
        self.peer_id = peer_id
        self.port = port
        self.tracker_url = tracker_url
        self.file_path = file_path
        
        # Componentes principais
        self.block_manager = BlockManager(peer_id)
        self.communication = PeerCommunication(peer_id, port)
        self.strategies = PeerStrategies(peer_id)
        
        # Estado do peer
        self.is_running = False
        self.is_seeder = False
        self.registered_with_tracker = False
        self.connected_peers: Set[str] = set()
        
        # Threading
        self.main_thread: Optional[threading.Thread] = None
        self.discovery_thread: Optional[threading.Thread] = None
        self.request_thread: Optional[threading.Thread] = None
        
        # Controle de timing
        self.last_peer_discovery = 0
        self.last_strategy_update = 0
        self.peer_discovery_interval = 30  # 30 segundos
        self.strategy_update_interval = 10  # 10 segundos
        
        # Estatísticas
        self.start_time = 0
        self.blocks_downloaded = 0
        self.blocks_uploaded = 0
        self.bytes_downloaded = 0
        self.bytes_uploaded = 0
        
        # Fila de requests
        self.pending_requests: Dict[str, Set[int]] = {}  # peer_id -> set de block_ids
        self.active_downloads: Dict[int, str] = {}  # block_id -> peer_id
        
        # Setup handlers
        self._setup_message_handlers()
        self._setup_signal_handlers()
    
    def start_peer(self):
        """Inicia o peer"""
        try:
            self.start_time = time.time()
            self.is_running = True
            
            logger.info(f"Iniciando peer {self.peer_id} na porta {self.port}")
            
            # Inicializar comunicação
            self.communication.start()
            
            # Carregar arquivo se fornecido
            if self.file_path:
                self._load_initial_file()
            
            # Registrar com tracker
            self._register_with_tracker()
            
            # Iniciar threads
            self.main_thread = threading.Thread(target=self._main_loop, daemon=True)
            self.discovery_thread = threading.Thread(target=self._peer_discovery_loop, daemon=True)
            self.request_thread = threading.Thread(target=self._request_processor_loop, daemon=True)
            
            self.main_thread.start()
            self.discovery_thread.start()
            self.request_thread.start()
            
            logger.info(f"Peer {self.peer_id} iniciado com sucesso")
            
            # Manter programa rodando
            self._wait_for_completion()
            
        except Exception as e:
            logger.error(f"Erro ao iniciar peer: {e}")
            raise
    
    def stop_peer(self):
        """Para o peer"""
        logger.info(f"Parando peer {self.peer_id}")
        
        self.is_running = False
        
        # Desregistrar do tracker
        self._unregister_from_tracker()
        
        # Parar comunicação
        self.communication.stop()
        
        logger.info(f"Peer {self.peer_id} parado")
    
    def join_network(self):
        """Conecta à rede P2P através do tracker"""
        try:
            # Descobrir peers
            peers = self._discover_peers()
            
            # Conectar aos peers
            connected_count = 0
            for peer_info in peers[:5]:  # Conectar a no máximo 5 peers
                peer_id = peer_info['peer_id']
                address = peer_info['address']
                port = peer_info['port']
                
                if self.communication.connect_to_peer(peer_id, address, port):
                    self.connected_peers.add(peer_id)
                    connected_count += 1
            
            logger.info(f"Conectado a {connected_count} peers")
            
            # Enviar bitfield inicial
            if not self.block_manager.is_complete():
                bitfield = self.block_manager.get_bitfield()
                self.communication.broadcast_bitfield(bitfield)
            
        except Exception as e:
            logger.error(f"Erro ao conectar à rede: {e}")
    
    def can_shutdown(self) -> bool:
        """Verifica se pode fazer shutdown (arquivo completo)"""
        return self.block_manager.is_complete()
    
    def _load_initial_file(self):
        """Carrega arquivo inicial se fornecido"""
        try:
            file_path = Path(self.file_path)
            if file_path.exists():
                total_blocks = self.block_manager.split_file_into_blocks(str(file_path))
                self.is_seeder = True
                logger.info(f"Arquivo carregado: {total_blocks} blocos, modo seeder")
            else:
                logger.warning(f"Arquivo não encontrado: {file_path}")
                
        except Exception as e:
            logger.error(f"Erro ao carregar arquivo inicial: {e}")
    
    def _register_with_tracker(self):
        """Registra peer no tracker"""
        try:
            data = {
                'peer_id': self.peer_id,
                'address': '127.0.0.1',  # Para desenvolvimento local
                'port': self.port,
                'total_blocks': self.block_manager.total_blocks
            }
            
            response = requests.post(f"{self.tracker_url}/register", json=data)
            response.raise_for_status()
            
            result = response.json()
            if result.get('success'):
                self.registered_with_tracker = True
                
                # Receber distribuição inicial de blocos se for novo peer
                initial_blocks = result.get('initial_blocks', [])
                if initial_blocks and not self.is_seeder:
                    logger.info(f"Blocos iniciais atribuídos: {initial_blocks}")
                    # Aqui você poderia implementar lógica para receber blocos iniciais
                
                logger.info("Registrado no tracker com sucesso")
            else:
                logger.error(f"Falha ao registrar no tracker: {result.get('message')}")
                
        except Exception as e:
            logger.error(f"Erro ao registrar no tracker: {e}")
    
    def _unregister_from_tracker(self):
        """Remove peer do tracker"""
        try:
            if not self.registered_with_tracker:
                return
                
            data = {'peer_id': self.peer_id}
            response = requests.post(f"{self.tracker_url}/unregister", json=data)
            response.raise_for_status()
            
            logger.info("Desregistrado do tracker")
            
        except Exception as e:
            logger.error(f"Erro ao desregistrar do tracker: {e}")
    
    def _discover_peers(self) -> List[Dict]:
        """Descobre peers através do tracker"""
        try:
            params = {'peer_id': self.peer_id}
            response = requests.get(f"{self.tracker_url}/peers", params=params)
            response.raise_for_status()
            
            result = response.json()
            if result.get('success'):
                peers = result.get('peers', [])
                logger.debug(f"Descobertos {len(peers)} peers")
                return peers
            else:
                logger.warning(f"Falha ao descobrir peers: {result.get('message')}")
                return []
                
        except Exception as e:
            logger.error(f"Erro ao descobrir peers: {e}")
            return []
    
    def _main_loop(self):
        """Loop principal do peer"""
        while self.is_running:
            try:
                current_time = time.time()
                
                # Atualizar estratégias periodicamente
                if current_time - self.last_strategy_update >= self.strategy_update_interval:
                    self._update_strategies()
                    self.last_strategy_update = current_time
                
                # Processar downloads se não for seeder
                if not self.is_seeder and not self.block_manager.is_complete():
                    self._process_downloads()
                
                # Verificar se arquivo está completo
                if not self.is_seeder and self.block_manager.is_complete():
                    self.is_seeder = True
                    logger.info("Download completo! Agora é seeder")
                    self._send_completion_notification()
                
                # Log de progresso
                if not self.is_seeder:
                    progress = self.block_manager.get_completion_percentage()
                    if int(progress) % 10 == 0:  # Log a cada 10%
                        logger.info(f"Progresso: {progress:.1f}%")
                
                time.sleep(1)  # Loop a cada segundo
                
            except Exception as e:
                logger.error(f"Erro no loop principal: {e}")
                time.sleep(5)
    
    def _peer_discovery_loop(self):
        """Loop de descoberta de peers"""
        while self.is_running:
            try:
                current_time = time.time()
                
                if current_time - self.last_peer_discovery >= self.peer_discovery_interval:
                    self._refresh_peer_connections()
                    self.last_peer_discovery = current_time
                
                time.sleep(10)  # Verificar a cada 10 segundos
                
            except Exception as e:
                logger.error(f"Erro no loop de descoberta: {e}")
                time.sleep(30)
    
    def _request_processor_loop(self):
        """Loop de processamento de requests"""
        while self.is_running:
            try:
                self._process_pending_requests()
                time.sleep(0.5)  # Processar requests rapidamente
                
            except Exception as e:
                logger.error(f"Erro no processador de requests: {e}")
                time.sleep(2)
    
    def _update_strategies(self):
        """Atualiza estratégias de unchoke/choke"""
        try:
            connected_peers = list(self.connected_peers)
            
            # Atualizar peers unchoked
            newly_unchoked, newly_choked = self.strategies.update_unchoked_peers(connected_peers)
            
            # Aplicar mudanças
            for peer_id in newly_unchoked:
                self.communication.send_unchoke(peer_id)
            
            for peer_id in newly_choked:
                self.communication.send_choke(peer_id)
            
            # Atualizar optimistic unchoke
            optimistic_peer = self.strategies.select_optimistic_unchoke(connected_peers)
            if optimistic_peer:
                self.communication.send_unchoke(optimistic_peer)
            
        except Exception as e:
            logger.error(f"Erro ao atualizar estratégias: {e}")
    
    def _process_downloads(self):
        """Processa downloads de blocos"""
        try:
            # Obter blocos em falta
            missing_blocks = self.block_manager.get_missing_blocks()
            if not missing_blocks:
                return
            
            # Selecionar blocos mais raros
            rarest_blocks = self.strategies.select_rarest_blocks(missing_blocks, max_blocks=10)
            
            # Fazer requests para blocos selecionados
            for block_id in rarest_blocks:
                if block_id in self.active_downloads:
                    continue  # Já está sendo baixado
                
                # Selecionar peers para este bloco
                candidate_peers = self.strategies.select_download_peers(block_id, max_peers=3)
                
                for peer_id in candidate_peers:
                    if self.strategies.should_request_from_peer(peer_id, block_id):
                        if peer_id not in self.pending_requests:
                            self.pending_requests[peer_id] = set()
                        self.pending_requests[peer_id].add(block_id)
                        self.communication.send_request(peer_id, block_id)
                        self.active_downloads[block_id] = peer_id
                        logger.info(f"Requesting block {block_id} from peer {peer_id}")
                        break
        except Exception as e:
            logger.error(f"Erro ao processar downloads: {e}")
            