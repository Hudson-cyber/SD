import socket
import threading
import json
import random
import logging

# Configura o log
logging.basicConfig(level=logging.INFO, format='[%(asctime)s] %(message)s')
# Dados dos peers: { (ip, porta): [lista_blocos] }
peers = {}
# Lock para garantir consistência entre threads
peers_lock = threading.Lock()

def handle_client(conn, addr):
    try:
        data = conn.recv(4096).decode()
        if not data:
            return

        msg = json.loads(data)
        peer_id = (addr[0], msg["port"])
        blocos = msg["blocks"]

        with peers_lock:
            peers[peer_id] = blocos
            logging.info(f"Peer registrado: {peer_id}, blocos: {blocos}")

        # Gera lista de peers
        with peers_lock:
            outros_peers = [ {"ip": ip, "port": port, "blocks": peers[(ip, port)]}
                             for (ip, port) in peers if (ip, port) != peer_id ]
            random.shuffle(outros_peers)
            resposta = outros_peers[:4] if len(outros_peers) > 4 else outros_peers

        conn.sendall(json.dumps(resposta).encode())

    except Exception as e:
        logging.error(f"Erro ao lidar com peer {addr}: {e}")
    finally:
        conn.close()

def start_tracker(host='0.0.0.0', port=5000):
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.bind((host, port))
    server.listen()

    logging.info(f"Tracker iniciado em {host}:{port}")

    while True:
        conn, addr = server.accept()
        thread = threading.Thread(target=handle_client, args=(conn, addr), daemon=True)
        thread.start()

if __name__ == "__main__":
    start_tracker()
