# config.py

# Central tracker settings
TRACKER_HOST = 'localhost'
TRACKER_PORT = 8000
TRACKER_TIMEOUT = 40  # Time in seconds to consider a peer inactive

# File name to distribute (customizable)
FILE_NAME = 'uerj.png'

# Block size in bytes
BLOCK_SIZE = 64

# Total blocks (calculated after splitting the file)
TOTAL_BLOCKS = None

# Network control parameters
UNCHOKE_INTERVAL = 10  # seconds
MAX_UNCHOKED = 4
OPTIMISTIC_UNCHOKE = 1

# Peer base port (peer 0 = 9000, peer 1 = 9001, etc.)
PEER_PORT_BASE = 9000

# How often a peer updates its tracker contact
TRACKER_UPDATE_INTERVAL = 15
