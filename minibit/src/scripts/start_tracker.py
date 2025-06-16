import socket

def start_tracker(host='0.0.0.0', port=8000):
    tracker = {}
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind((host, port))
    print(f"Tracker iniciado em {host}:{port}")

    try:
        while True:
            data, addr = sock.recvfrom(1024)
            message = data.decode()
            if message.startswith("REGISTER"):
                peer_info = message.split()[1]
                tracker[peer_info] = addr
                sock.sendto(b"REGISTERED", addr)
                print(f"Peer registrado: {peer_info} de {addr}")
            elif message.startswith("GET_PEERS"):
                peers = ",".join(tracker.keys())
                sock.sendto(peers.encode(), addr)
            else:
                sock.sendto(b"COMANDO INVALIDO", addr)
    except KeyboardInterrupt:
        print("\nTracker finalizado.")
    finally:
        sock.close()

if __name__ == "__main__":
    start_tracker()