from flask import Flask, jsonify, request
import random
from collections import defaultdict

app = Flask(__name__)
active_peers = {}  # {peer_id: {"ip": str, "port": int, "blocks": list[int]}}

@app.route('/register', methods=['POST'])
def register_peer():
    data = request.json
    peer_id = data["peer_id"]
    active_peers[peer_id] = {
        "ip": data["ip"],
        "port": data["port"],
        "blocks": data["blocks"]
    }
    return jsonify({"status": "registered"})

@app.route('/get_peers', methods=['GET'])
def get_peers():
    requesting_peer = request.args.get("peer_id")
    if requesting_peer not in active_peers:
        return jsonify({"error": "peer not registered"}), 400
    
    # Obter blocos que o peer precisa
    requesting_blocks = set(active_peers[requesting_peer]["blocks"])
    all_blocks = set()
    for peer in active_peers.values():
        all_blocks.update(peer["blocks"])
    needed_blocks = all_blocks - requesting_blocks
    
    # Encontrar peers que possuem blocos necessários
    useful_peers = {}
    block_distribution = defaultdict(list)
    
    for peer_id, info in active_peers.items():
        if peer_id == requesting_peer:
            continue
        
        common_blocks = set(info["blocks"]) & needed_blocks
        if common_blocks:
            useful_peers[peer_id] = info
            for block in common_blocks:
                block_distribution[block].append(peer_id)
    
    # Priorizar peers com blocos mais raros
    peer_scores = defaultdict(int)
    for block, peers in block_distribution.items():
        rarity = 1 / len(peers)  # Quanto menos peers, mais raro
        for peer in peers:
            peer_scores[peer] += rarity
    
    # Ordenar peers por score
    sorted_peers = sorted(
        useful_peers.items(),
        key=lambda x: peer_scores[x[0]],
        reverse=True
    )
    
    # Retornar até 50 peers (BitTorrent padrão)
    max_peers = min(50, len(sorted_peers))
    selected_peers = dict(sorted_peers[:max_peers])
    
    return jsonify({"peers": selected_peers})

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)