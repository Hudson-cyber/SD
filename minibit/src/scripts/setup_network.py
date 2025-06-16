import os
import subprocess

def create_network(network_name, subnet, gateway):
    """
    Cria uma rede Docker bridge personalizada.
    """
    cmd = [
        "docker", "network", "create",
        "--driver", "bridge",
        "--subnet", subnet,
        "--gateway", gateway,
        network_name
    ]
    try:
        subprocess.run(cmd, check=True)
        print(f"Rede '{network_name}' criada com sucesso.")
    except subprocess.CalledProcessError as e:
        print(f"Erro ao criar a rede: {e}")

def main():
    network_name = "minibit_network"
    subnet = "172.25.0.0/16"
    gateway = "172.25.0.1"
    create_network(network_name, subnet, gateway)

if __name__ == "__main__":
    main()