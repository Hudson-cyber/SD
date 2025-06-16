import socket
import threading
import json
import logging
import time
from typing import Dict, List, Optional, Callable, Set
from queue import Queue, Empty
import struct

from SD.minibit.src.common.messages import Message, MessageType

logger = logging.getLogger(__name__)

class PeerConnection:
    def __init__(self, peer_id: str, socket_conn: socket.socket, address: tuple):
        self.peer_id = peer_id
        self.socket = socket_conn
        self.address = address
        self.is_active = True
        self.last_activity = time.time()
        self.choked = True  # Inicialmente choked
        self.interested = False
        self.bitfield: Optional[bytes] = None
        self.blocks_owned: Set[int] = set()
        
    def send_message(self, message: Message) -> bool:
        """Envia uma mensagem para o peer"""
        try:
            if not self.is_active:
                return False
                
            # Serializar mensagem
            data = message.serialize()
            
            # Enviar tamanho da mensagem primeiro (4 bytes)
            length = struct.pack('>I', len(data))
            self.socket.send(length + data)
            
            self.last_activity = time.time()
            logger.debug(f"Mensagem {message.type} enviada para {self.peer_id}")
            return True
            
        except Exception as e:
            logger.error(f"Erro ao enviar mensagem para {self.peer_id}: {e}")
            self.is_active = False
            return False
    
    def receive_message(self) -> Optional[Message]:
        """Recebe uma mensagem do peer"""
        try:
            if not self.is_active:
                return None
            
            # Receber tamanho da mensagem (4 bytes)
            length_data = self._recv_exact(4)
            if not length_data:
                return None
            
            length = struct.unpack('>I', length_data)[0]
            if length == 0:  # Keep-alive message
                return Message(MessageType.KEEPALIVE, {})
            
            # Receber dados da mensagem
            message_data = self._recv_exact(length)
            if not message_data:
                return None
            
            message = Message.deserialize(message_data)
            self.last_activity = time.time()
            
            logger.debug(f"Mensagem {message.type} recebida de {self.peer_id}")
            return message
            
        except Exception as e:
            logger.error(f"Erro ao receber mensagem de {self.peer_id}: {e}")
            self.is_active = False
            return None
    
    def _recv_exact(self, size: int) -> Optional[bytes]:
        """Recebe exatamente 'size' bytes"""
        data = b''
        while len(data) < size:
            try:
                chunk = self.socket.recv(size - len(data))
                if not chunk:
                    return None
                data += chunk
            except Exception:
                return None
        return data
    
    def close(self):
        """Fecha a conexão"""
        self.is_active = False
        try:
            self.socket.close()
        except:
            pass


class PeerCommunication:
    def __init__(self, peer_id: str, port: int):
        self.peer_id = peer_id
        self.port = port
        self.connections: Dict[str, PeerConnection] = {}
        self.server_socket: Optional[socket.socket] = None
        self.is_running = False
        self.message_handlers: Dict[str, Callable] = {}
        self.message_queue: Queue = Queue()
        
        # Threading
        self.server_thread: Optional[threading.Thread] = None
        self.message_thread: Optional[threading.Thread] = None
        self.connection_threads: Dict[str, threading.Thread] = {}
        
        # Estatísticas
        self.bytes_sent = 0
        self.bytes_received = 0
        self.messages_sent = 0
        self.messages_received = 0
        
    def start(self):
        """Inicia o sistema de comunicação"""
        try:
            self.is_running = True
            
            # Iniciar servidor
            self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            self.server_socket.bind(('0.0.0.0', self.port))
            self.server_socket.listen(10)
            
            # Thread do servidor
            self.server_thread = threading.Thread(target=self._server_loop, daemon=True)
            self.server_thread.start()
            
            # Thread de processamento de mensagens
            self.message_thread = threading.Thread(target=self._message_processor, daemon=True)
            self.message_thread.start()
            
            logger.info(f"Peer {self.peer_id} iniciado na porta {self.port}")
            
        except Exception as e:
            logger.error(f"Erro ao iniciar comunicação: {e}")
            raise
    
    def stop(self):
        """Para o sistema de comunicação"""
        self.is_running = False
        
        # Fechar todas as conexões
        for connection in list(self.connections.values()):
            connection.close()
        self.connections.clear()
        
        # Fechar servidor
        if self.server_socket:
            try:
                self.server_socket.close()
            except:
                pass
        
        logger.info(f"Peer {self.peer_id} parado")
    
    def connect_to_peer(self, peer_id: str, address: str, port: int) -> bool:
        """Conecta a outro peer"""
        try:
            if peer_id in self.connections:
                logger.debug(f"Já conectado ao peer {peer_id}")
                return True
            
            # Criar socket e conectar
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(10)  # Timeout de 10 segundos
            sock.connect((address, port))
            sock.settimeout(None)
            
            # Criar conexão
            connection = PeerConnection(peer_id, sock, (address, port))
            self.connections[peer_id] = connection
            
            # Enviar handshake
            handshake_msg = Message(MessageType.HANDSHAKE, {
                'peer_id': self.peer_id,
                'protocol': 'minibit-1.0'
            })
            
            if not connection.send_message(handshake_msg):
                del self.connections[peer_id]
                return False
            
            # Iniciar thread para esta conexão
            thread = threading.Thread(
                target=self._handle_peer_connection, 
                args=(connection,), 
                daemon=True
            )
            thread.start()
            self.connection_threads[peer_id] = thread
            
            logger.info(f"Conectado ao peer {peer_id} em {address}:{port}")
            return True
            
        except Exception as e:
            logger.error(f"Erro ao conectar com peer {peer_id}: {e}")
            return False
    
    def disconnect_peer(self, peer_id: str):
        """Desconecta de um peer"""
        if peer_id in self.connections:
            self.connections[peer_id].close()
            del self.connections[peer_id]
            
            if peer_id in self.connection_threads:
                del self.connection_threads[peer_id]
            
            logger.info(f"Desconectado do peer {peer_id}")
    
    def send_have_message(self, block_id: int, target_peer: Optional[str] = None):
        """Envia mensagem HAVE sobre um bloco"""
        message = Message(MessageType.HAVE, {'block_id': block_id})
        
        if target_peer:
            self._send_to_peer(target_peer, message)
        else:
            self._broadcast_message(message)
    
    def request_block(self, peer_id: str, block_id: int) -> bool:
        """Solicita um bloco específico de um peer"""
        message = Message(MessageType.REQUEST, {'block_id': block_id})
        return self._send_to_peer(peer_id, message)
    
    def send_block(self, peer_id: str, block_id: int, data: bytes) -> bool:
        """Envia um bloco para um peer"""
        message = Message(MessageType.PIECE, {
            'block_id': block_id,
            'data': data
        })
        return self._send_to_peer(peer_id, message)
    
    def broadcast_bitfield(self, bitfield: bytes):
        """Envia bitfield para todos os peers conectados"""
        message = Message(MessageType.BITFIELD, {'bitfield': bitfield})
        self._broadcast_message(message)
    
    def send_choke(self, peer_id: str):
        """Envia mensagem de choke"""
        message = Message(MessageType.CHOKE, {})
        if self._send_to_peer(peer_id, message):
            if peer_id in self.connections:
                self.connections[peer_id].choked = True
    
    def send_unchoke(self, peer_id: str):
        """Envia mensagem de unchoke"""
        message = Message(MessageType.UNCHOKE, {})
        if self._send_to_peer(peer_id, message):
            if peer_id in self.connections:
                self.connections[peer_id].choked = False
    
    def send_interested(self, peer_id: str):
        """Envia mensagem de interesse"""
        message = Message(MessageType.INTERESTED, {})
        if self._send_to_peer(peer_id, message):
            if peer_id in self.connections:
                self.connections[peer_id].interested = True
    
    def send_not_interested(self, peer_id: str):
        """Envia mensagem de não interesse"""
        message = Message(MessageType.NOT_INTERESTED, {})
        if self._send_to_peer(peer_id, message):
            if peer_id in self.connections:
                self.connections[peer_id].interested = False
    
    def register_message_handler(self, message_type: str, handler: Callable):
        """Registra handler para tipo de mensagem"""
        self.message_handlers[message_type] = handler
    
    def get_connected_peers(self) -> List[str]:
        """Retorna lista de peers conectados"""
        return [peer_id for peer_id, conn in self.connections.items() if conn.is_active]
    
    def get_peer_blocks(self, peer_id: str) -> Set[int]:
        """Retorna blocos que um peer possui"""
        if peer_id in self.connections:
            return self.connections[peer_id].blocks_owned.copy()
        return set()
    
    def is_peer_choked(self, peer_id: str) -> bool:
        """Verifica se peer está choked"""
        if peer_id in self.connections:
            return self.connections[peer_id].choked
        return True
    
    def _server_loop(self):
        """Loop principal do servidor"""
        while self.is_running:
            try:
                client_socket, address = self.server_socket.accept()
                
                # Criar thread para lidar com nova conexão
                thread = threading.Thread(
                    target=self._handle_incoming_connection,
                    args=(client_socket, address),
                    daemon=True
                )
                thread.start()
                
            except Exception as e:
                if self.is_running:
                    logger.error(f"Erro no servidor: {e}")
    
    def _handle_incoming_connection(self, client_socket: socket.socket, address: tuple):
        """Lida com conexão entrante"""
        try:
            # Criar conexão temporária para handshake
            temp_connection = PeerConnection("unknown", client_socket, address)
            
            # Aguardar handshake
            handshake_msg = temp_connection.receive_message()
            if not handshake_msg or handshake_msg.type != MessageType.HANDSHAKE:
                client_socket.close()
                return
            
            peer_id = handshake_msg.data.get('peer_id')
            if not peer_id or peer_id == self.peer_id:
                client_socket.close()
                return
            
            # Responder handshake
            response = Message(MessageType.HANDSHAKE, {
                'peer_id': self.peer_id,
                'protocol': 'minibit-1.0'
            })
            temp_connection.send_message(response)
            
            # Registrar conexão
            connection = PeerConnection(peer_id, client_socket, address)
            self.connections[peer_id] = connection
            
            logger.info(f"Peer {peer_id} conectou de {address}")
            
            # Processar mensagens desta conexão
            self._handle_peer_connection(connection)
            
        except Exception as e:
            logger.error(f"Erro ao processar conexão entrante: {e}")
            try:
                client_socket.close()
            except:
                pass
    
    def _handle_peer_connection(self, connection: PeerConnection):
        """Processa mensagens de uma conexão específica"""
        while self.is_running and connection.is_active:
            try:
                message = connection.receive_message()
                if message:
                    self.message_queue.put((connection.peer_id, message))
                    self.messages_received += 1
                else:
                    # Conexão perdida
                    break
                    
            except Exception as e:
                logger.error(f"Erro ao processar mensagens de {connection.peer_id}: {e}")
                break
        
        # Limpar conexão
        connection.close()
        if connection.peer_id in self.connections:
            del self.connections[connection.peer_id]
    
    def _message_processor(self):
        """Processa mensagens da fila"""
        while self.is_running:
            try:
                peer_id, message = self.message_queue.get(timeout=1)
                
                # Atualizar estado da conexão baseado na mensagem
                self._update_connection_state(peer_id, message)
                
                # Chamar handler se registrado
                if message.type in self.message_handlers:
                    try:
                        self.message_handlers[message.type](peer_id, message)
                    except Exception as e:
                        logger.error(f"Erro no handler {message.type}: {e}")
                
            except Empty:
                continue
            except Exception as e:
                logger.error(f"Erro no processador de mensagens: {e}")
    
    def _update_connection_state(self, peer_id: str, message: Message):
        """Atualiza estado da conexão baseado na mensagem"""
        if peer_id not in self.connections:
            return
        
        connection = self.connections[peer_id]
        
        if message.type == MessageType.BITFIELD:
            connection.bitfield = message.data.get('bitfield', b'')
            # Converter bitfield para set de blocos
            connection.blocks_owned.clear()
            bitfield = connection.bitfield
            for block_id in range(len(bitfield) * 8):
                byte_idx = block_id // 8
                bit_idx = block_id % 8
                if byte_idx < len(bitfield) and (bitfield[byte_idx] & (1 << (7 - bit_idx))):
                    connection.blocks_owned.add(block_id)
        
        elif message.type == MessageType.HAVE:
            block_id = message.data.get('block_id')
            if block_id is not None:
                connection.blocks_owned.add(block_id)
        
        elif message.type == MessageType.CHOKE:
            connection.choked = True
        
        elif message.type == MessageType.UNCHOKE:
            connection.choked = False
        
        elif message.type == MessageType.INTERESTED:
            connection.interested = True
        
        elif message.type == MessageType.NOT_INTERESTED:
            connection.interested = False
    
    def _send_to_peer(self, peer_id: str, message: Message) -> bool:
        """Envia mensagem para peer específico"""
        if peer_id in self.connections:
            success = self.connections[peer_id].send_message(message)
            if success:
                self.messages_sent += 1
            return success
        return False
    
    def _broadcast_message(self, message: Message):
        """Envia mensagem para todos os peers conectados"""
        for peer_id in list(self.connections.keys()):
            self._send_to_peer(peer_id, message)
    
    def get_statistics(self) -> Dict:
        """Retorna estatísticas de comunicação"""
        return {
            'connected_peers': len(self.connections),
            'messages_sent': self.messages_sent,
            'messages_received': self.messages_received,
            'bytes_sent': self.bytes_sent,
            'bytes_received': self.bytes_received
        }