# src/tracker/tracker_api.py
"""
API cliente para comunicação com o Tracker
"""
import socket
import json
import logging
from typing import List, Dict, Optional, Tuple
from ..common.config import *
from ..common.messages import Message, MessageBuilder

class TrackerAPI:
    """Cliente para comunicação com o servidor tracker"""
    
    def __init__(self, tracker_host: str = TRACKER_HOST, tracker_port: int = TRACKER_PORT):
        self.tracker_host = tracker_host
        self.tracker_port = tracker_port
        self.logger = logging.getLogger(f"TrackerAPI")
        
    def _send_message(self, message: Message) -> Optional[Message]:
        """
        Envia uma mensagem para o tracker e retorna a resposta
        
        Args:
            message: Mensagem para enviar
            
        Returns:
            Resposta do tracker ou None em caso de erro
        """
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                sock.settimeout(CONNECTION_TIMEOUT)
                sock.connect((self.tracker_host, self.tracker_port))
                
                # Envia mensagem
                sock.send(message.to_json().encode('utf-8'))
                
                # Recebe resposta
                response_data = sock.recv(4096).decode('utf-8')
                if response_data:
                    return Message.from_json(response_data)
                    
        except socket.timeout:
            self.logger.error("Timeout na comunicação com tracker")
        except ConnectionRefusedError:
            self.logger.error("Conexão recusada pelo tracker")
        except Exception as e:
            self.logger.error(f"Erro na comunicação com tracker: {e}")
            
        return None
    
    def register_peer(self, peer_id: str, host: str, port: int, 
                     file_hash: str = "default", total_blocks: int = 10) -> Optional[List[int]]:
        """
        Registra um peer no tracker
        
        Args:
            peer_id: ID único do peer
            host: Host do peer
            port: Porta do peer
            file_hash: Hash do arquivo sendo compartilhado
            total_blocks: Número total de blocos do arquivo
            
        Returns:
            Lista de blocos iniciais ou None em caso de erro
        """
        message = Message(
            type="register_peer",
            peer_id=peer_id,
            timestamp=0,
            data={
                'host': host,
                'port': port,
                'file_hash': file_hash,
                'total_blocks': total_blocks
            }
        )
        
        response = self._send_message(message)
        if response and response.data.get('success'):
            initial_blocks = response.data.get('initial_blocks', [])
            self.logger.info(f"Peer {peer_id} registrado com blocos: {initial_blocks}")
            return initial_blocks
        
        self.logger.error(f"Falha ao registrar peer {peer_id}")
        return None
    
    def get_peers(self, peer_id: str, file_hash: str = "default", 
                 max_peers: int = TRACKER_MAX_PEERS_RETURNED) -> List[Dict]:
        """
        Obtém lista de peers disponíveis
        
        Args:
            peer_id: ID do peer solicitante
            file_hash: Hash do arquivo
            max_peers: Número máximo de peers para retornar
            
        Returns:
            Lista de informações de peers
        """
        message = Message(
            type="get_peers",
            peer_id=peer_id,
            timestamp=0,
            data={
                'file_hash': file_hash,
                'max_peers': max_peers
            }
        )
        
        response = self._send_message(message)
        if response:
            peers = response.data.get('peers', [])
            self.logger.info(f"Recebidos {len(peers)} peers para {peer_id}")
            return peers
        
        self.logger.error(f"Falha ao obter peers para {peer_id}")
        return []
    
    def remove_peer(self, peer_id: str) -> bool:
        """
        Remove um peer do tracker
        
        Args:
            peer_id: ID do peer a ser removido
            
        Returns:
            True se removido com sucesso, False caso contrário
        """
        message = Message(
            type="remove_peer",
            peer_id=peer_id,
            timestamp=0,
            data={}
        )
        
        response = self._send_message(message)
        if response and response.data.get('success'):
            self.logger.info(f"Peer {peer_id} removido do tracker")
            return True
        
        self.logger.error(f"Falha ao remover peer {peer_id}")
        return False
    
    def send_heartbeat(self, peer_id: str) -> bool:
        """
        Envia heartbeat para o tracker
        
        Args:
            peer_id: ID do peer
            
        Returns:
            True se heartbeat foi aceito, False caso contrário
        """
        message = Message(
            type="heartbeat",
            peer_id=peer_id,
            timestamp=0,
            data={}
        )
        
        response = self._send_message(message)
        if response and response.data.get('success'):
            return True
        
        return False
    
    def is_tracker_alive(self) -> bool:
        """
        Verifica se o tracker está ativo
        
        Returns:
            True se tracker está respondendo, False caso contrário
        """
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                sock.settimeout(5)  # timeout curto para verificação
                result = sock.connect_ex((self.tracker_host, self.tracker_port))
                return result == 0
        except Exception:
            return False