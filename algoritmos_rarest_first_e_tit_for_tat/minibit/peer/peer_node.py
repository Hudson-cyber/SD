import socket
import threading
import requests
import json
import time
from file_manager import split_file, reconstruct_file
from algorithms import RarestFirst, TitForTat

class Peer:
    def __init__(self, peer_id, file_path, tracker_url):
        self.id = peer_id
        self.blocks, self.total_blocks = split_file(file_path)
        self.tracker_url = tracker_url
        self.known_peers = {}
        self.server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server.bind(('localhost', 0))
        self.server.listen(5)
        self.tit_for_tat = TitForTat()
        self.lock = threading.Lock()
        self.active_downloads = {}
        self.last_peer_update = 0
        
        # Registrar no tracker
        self.register_with_tracker()
        
        # Threads
        threading.Thread(target=self.listen_connections, daemon=True).start()
        threading.Thread(target=self.download_manager, daemon=True).start()
        threading.Thread(target=self.unchoke_manager, daemon=True).start()
    
    def register_with_tracker(self):
        requests.post(f"{self.tracker_url}/register", json={
            "peer_id": self.id,
            "ip": self.server.getsockname()[0],
            "port": self.server.getsockname()[1],
            "blocks": self.blocks
        })
    
    def update_peer_list(self):
        if time.time() - self.last_peer_update > 30:  # Atualizar a cada 30s
            response = requests.get(
                f"{self.tracker_url}/get_peers",
                params={"peer_id": self.id}
            )
            if response.status_code == 200:
                self.known_peers = response.json().get("peers", {})
            self.last_peer_update = time.time()
    
    def get_needed_blocks(self):
        return [b for b in range(self.total_blocks) if b not in self.blocks]
    
    def listen_connections(self):
        while True:
            client, addr = self.server.accept()
            threading.Thread(target=self.handle_request, args=(client,)).start()
    
    def handle_request(self, client):
        data = client.recv(1024).decode()
        try:
            message = json.loads(data)
            
            if message["type"] == "request_block":
                block_id = message["block_id"]
                if block_id in self.blocks:
                    # Enviar bloco e atualizar taxa de upload
                    with open(f"block_{block_id}.bin", 'rb') as f:
                        block_data = f.read()
                    client.sendall(block_data)
                    self.tit_for_tat.update_upload_rate(
                        message["sender_id"],
                        len(block_data)
                    )
            
            elif message["type"] == "choke":
                peer_id = message["sender_id"]
                with self.lock:
                    if peer_id in self.active_downloads:
                        self.active_downloads[peer_id]["choked"] = True
            
            elif message["type"] == "unchoke":
                peer_id = message["sender_id"]
                with self.lock:
                    if peer_id in self.active_downloads:
                        self.active_downloads[peer_id]["choked"] = False
        
        except Exception as e:
            print(f"Erro ao processar mensagem: {e}")
        finally:
            client.close()
    
    def download_manager(self):
        while not self.check_completion():
            self.update_peer_list()
            needed_blocks = self.get_needed_blocks()
            
            if not needed_blocks:
                time.sleep(5)
                continue
            
            # Selecionar blocos mais raros e peers que os possuem
            rarest_blocks, peer_map = RarestFirst.select_blocks(self)
            
            if not rarest_blocks:
                time.sleep(5)
                continue
            
            # Tentar baixar de peers unchoked
            unchoked_peers = self.tit_for_tat.update_unchoked(self)
            
            for block in rarest_blocks:
                for peer_id in peer_map.get(block, []):
                    if peer_id in unchoked_peers:
                        self.download_block(block, peer_id)
                        break
            
            time.sleep(1)
    
    def download_block(self, block_id, peer_id):
        with self.lock:
            if block_id in self.blocks or peer_id not in self.known_peers:
                return
            
            if peer_id not in self.active_downloads:
                self.active_downloads[peer_id] = {
                    "choked": True,
                    "last_request": 0
                }
            
            peer_info = self.known_peers[peer_id]
            
        # Verificar se não está choked e respeitar intervalo entre requests
        if (self.active_downloads[peer_id]["choked"] or 
            time.time() - self.active_downloads[peer_id]["last_request"] < 1):
            return
        
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.connect((peer_info["ip"], peer_info["port"]))
            
            request = {
                "type": "request_block",
                "block_id": block_id,
                "sender_id": self.id
            }
            s.sendall(json.dumps(request).encode())
            
            # Receber dados
            data = b""
            while True:
                chunk = s.recv(4096)
                if not chunk:
                    break
                data += chunk
            
            if data:
                # Salvar bloco
                with open(f"block_{block_id}.bin", 'wb') as f:
                    f.write(data)
                
                # Atualizar estado
                with self.lock:
                    self.blocks.append(block_id)
                    self.tit_for_tat.update_download_rate(
                        peer_id,
                        len(data)
                    )
                
                print(f"Baixado bloco {block_id} de {peer_id}")
        
        except Exception as e:
            print(f"Erro ao baixar bloco {block_id} de {peer_id}: {e}")
        
        finally:
            s.close()
            with self.lock:
                self.active_downloads[peer_id]["last_request"] = time.time()
    
    def unchoke_manager(self):
        while True:
            unchoked_peers = self.tit_for_tat.update_unchoked(self)
            
            # Notificar peers sobre unchoke
            for peer_id in unchoked_peers:
                if peer_id in self.known_peers:
                    self.send_message(peer_id, {"type": "unchoke"})
            
            # Notificar peers choked que não estão mais na lista
            current_unchoked = set(unchoked_peers)
            with self.lock:
                for peer_id in list(self.active_downloads.keys()):
                    if peer_id not in current_unchoked:
                        self.send_message(peer_id, {"type": "choke"})
            
            time.sleep(10)
    
    def send_message(self, peer_id, message):
        if peer_id not in self.known_peers:
            return
        
        peer_info = self.known_peers[peer_id]
        message["sender_id"] = self.id
        
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.connect((peer_info["ip"], peer_info["port"]))
            s.sendall(json.dumps(message).encode())
            s.close()
        except Exception as e:
            print(f"Erro ao enviar mensagem para {peer_id}: {e}")
    
    def check_completion(self):
        try:
            reconstruct_file(self.blocks, self.total_blocks, "output_file.bin")
            print("Arquivo completo! Pode sair do sistema.")
            return True
        except ValueError as e:
            return False