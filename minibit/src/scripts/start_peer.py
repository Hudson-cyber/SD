import argparse

def start_peer(port, bootstrap_ip=None, bootstrap_port=None):
    print(f"Iniciando peer na porta {port}")
    if bootstrap_ip and bootstrap_port:
        print(f"Conectando ao bootstrap {bootstrap_ip}:{bootstrap_port}")
    else:
        print("Nenhum bootstrap informado. Este peer será o bootstrap.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Inicia um peer na rede P2P.")
    parser.add_argument('--port', type=int, required=True, help='Porta do peer')
    parser.add_argument('--bootstrap-ip', type=str, help='IP do peer bootstrap')
    parser.add_argument('--bootstrap-port', type=int, help='Porta do peer bootstrap')
    args = parser.parse_args()

    start_peer(args.port, args.bootstrap_ip, args.bootstrap_port)