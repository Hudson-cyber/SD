# src/common/config.py
"""
Configurações globais do sistema MiniBit
"""

# Configurações do Tracker
TRACKER_HOST = "localhost"
TRACKER_PORT = 7000
TRACKER_MAX_PEERS_RETURNED = 5

# Configurações dos Peers
PEER_BASE_PORT = 8000
BLOCK_SIZE = 1024  # 1KB por bloco
MAX_CONNECTIONS = 10
CONNECTION_TIMEOUT = 30

# Configurações das Estratégias
UNCHOKE_INTERVAL = 10  # segundos
MAX_UNCHOKED_PEERS = 4
OPTIMISTIC_UNCHOKE_COUNT = 1

# Configurações de Rede
HEARTBEAT_INTERVAL = 30  # segundos
REQUEST_TIMEOUT = 10
MAX_RETRIES = 3

# Configurações de Logs
LOG_LEVEL = "INFO"
LOG_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"

# Diretórios
ORIGINAL_FILES_DIR = "files/original"
RECONSTRUCTED_FILES_DIR = "files/reconstructed"
LOGS_DIR = "logs"

# Estados dos Peers
PEER_STATES = {
    'STARTING': 'starting',
    'CONNECTING': 'connecting', 
    'DOWNLOADING': 'downloading',
    'SEEDING': 'seeding',
    'STOPPING': 'stopping'
}