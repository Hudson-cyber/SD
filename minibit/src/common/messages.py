# src/common/messages.py
"""
Definições das mensagens do protocolo MiniBit
"""
import json
import time
from enum import Enum
from dataclasses import dataclass, asdict
from typing import List, Dict, Any, Optional

class MessageType(Enum):
    # Mensagens do Tracker
    REGISTER_PEER = "register_peer"
    GET_PEERS = "get_peers" 
    REMOVE_PEER = "remove_peer"
    HEARTBEAT = "heartbeat"
    
    # Mensagens P2P
    HANDSHAKE = "handshake"
    BITFIELD = "bitfield"
    HAVE = "have"
    REQUEST = "request"
    PIECE = "piece"
    CHOKE = "choke"
    UNCHOKE = "unchoke"
    INTERESTED = "interested"
    NOT_INTERESTED = "not_interested"

@dataclass
class Message:
    type: str
    peer_id: str
    timestamp: float
    data: Dict[str, Any]
    
    def to_json(self) -> str:
        return json.dumps(asdict(self))
    
    @classmethod
    def from_json(cls, json_str: str) -> 'Message':
        data = json.loads(json_str)
        return cls(**data)

class MessageBuilder:
    """Construtor de mensagens do protocolo"""
    
    @staticmethod
    def register_peer(peer_id: str, host: str, port: int, file_hash: str = None) -> Message:
        return Message(
            type=MessageType.REGISTER_PEER.value,
            peer_id=peer_id,
            timestamp=time.time(),
            data={
                'host': host,
                'port': port,
                'file_hash': file_hash
            }
        )
    
    @staticmethod
    def get_peers(peer_id: str, file_hash: str = None) -> Message:
        return Message(
            type=MessageType.GET_PEERS.value,
            peer_id=peer_id,
            timestamp=time.time(),
            data={
                'file_hash': file_hash,
                'max_peers': 5
            }
        )
    
    @staticmethod
    def handshake(peer_id: str, file_hash: str = None) -> Message:
        return Message(
            type=MessageType.HANDSHAKE.value,
            peer_id=peer_id,
            timestamp=time.time(),
            data={
                'file_hash': file_hash,
                'protocol_version': '1.0'
            }
        )
    
    @staticmethod
    def bitfield(peer_id: str, bitfield: List[bool]) -> Message:
        return Message(
            type=MessageType.BITFIELD.value,
            peer_id=peer_id,
            timestamp=time.time(),
            data={
                'bitfield': bitfield
            }
        )
    
    @staticmethod
    def have(peer_id: str, block_id: int) -> Message:
        return Message(
            type=MessageType.HAVE.value,
            peer_id=peer_id,
            timestamp=time.time(),
            data={
                'block_id': block_id
            }
        )
    
    @staticmethod
    def request(peer_id: str, block_id: int) -> Message:
        return Message(
            type=MessageType.REQUEST.value,
            peer_id=peer_id,  
            timestamp=time.time(),
            data={
                'block_id': block_id
            }
        )
    
    @staticmethod
    def piece(peer_id: str, block_id: int, block_data: bytes) -> Message:
        import base64
        return Message(
            type=MessageType.PIECE.value,
            peer_id=peer_id,
            timestamp=time.time(),
            data={
                'block_id': block_id,
                'block_data': base64.b64encode(block_data).decode('utf-8')
            }
        )
    
    @staticmethod
    def choke(peer_id: str) -> Message:
        return Message(
            type=MessageType.CHOKE.value,
            peer_id=peer_id,
            timestamp=time.time(),
            data={}
        )
    
    @staticmethod
    def unchoke(peer_id: str) -> Message:
        return Message(
            type=MessageType.UNCHOKE.value,
            peer_id=peer_id,
            timestamp=time.time(),
            data={}
        )
    
    @staticmethod
    def interested(peer_id: str) -> Message:
        return Message(
            type=MessageType.INTERESTED.value,
            peer_id=peer_id,
            timestamp=time.time(),
            data={}
        )
    
    @staticmethod
    def not_interested(peer_id: str) -> Message:
        return Message(
            type=MessageType.NOT_INTERESTED.value,
            peer_id=peer_id,
            timestamp=time.time(),
            data={}
        )

class MessageParser:
    """Parser de mensagens recebidas"""
    
    @staticmethod
    def parse_piece_data(message: Message) -> bytes:
        """Extrai dados binários de uma mensagem PIECE"""
        import base64
        encoded_data = message.data.get('block_data', '')
        return base64.b64decode(encoded_data)
    
    @staticmethod
    def validate_message(message: Message) -> bool:
        """Valida se uma mensagem está bem formada"""
        if not isinstance(message.type, str):
            return False
        if not isinstance(message.peer_id, str):
            return False
        if not isinstance(message.timestamp, (int, float)):
            return False
        if not isinstance(message.data, dict):
            return False
        return True