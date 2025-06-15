# src/common/file_utils.py
"""
Utilitários para manipulação de arquivos no MiniBit
"""
import os
import hashlib
import pickle
from typing import List, Dict, Optional
from .config import BLOCK_SIZE, ORIGINAL_FILES_DIR, RECONSTRUCTED_FILES_DIR, LOGS_DIR

class FileUtils:
    """Utilitários para operações com arquivos"""
    
    @staticmethod
    def ensure_directories():
        """Cria diretórios necessários se não existirem"""
        dirs = [ORIGINAL_FILES_DIR, RECONSTRUCTED_FILES_DIR, LOGS_DIR]
        for directory in dirs:
            os.makedirs(directory, exist_ok=True)
    
    @staticmethod
    def split_file_to_blocks(file_path: str, block_size: int = BLOCK_SIZE) -> List[bytes]:
        """
        Divide um arquivo em blocos de tamanho fixo
        
        Args:
            file_path: Caminho para o arquivo
            block_size: Tamanho de cada bloco em bytes
            
        Returns:
            Lista de blocos (bytes)
        """
        blocks = []
        try:
            with open(file_path, 'rb') as file:
                while True:
                    block = file.read(block_size)
                    if not block:
                        break
                    blocks.append(block)
        except FileNotFoundError:
            raise FileNotFoundError(f"Arquivo não encontrado: {file_path}")
        except Exception as e:
            raise Exception(f"Erro ao ler arquivo: {e}")
        
        return blocks
    
    @staticmethod
    def reconstruct_file_from_blocks(blocks: Dict[int, bytes], output_path: str) -> bool:
        """
        Reconstrói um arquivo a partir dos blocos
        
        Args:
            blocks: Dicionário {block_id: block_data}
            output_path: Caminho para salvar o arquivo reconstruído
            
        Returns:
            True se sucesso, False caso contrário
        """
        try:
            # Ordena os blocos por ID
            sorted_blocks = sorted(blocks.items())
            
            with open(output_path, 'wb') as file:
                for block_id, block_data in sorted_blocks:
                    file.write(block_data)
            
            return True
        except Exception as e:
            print(f"Erro ao reconstruir arquivo: {e}")
            return False
    
    @staticmethod
    def calculate_file_hash(file_path: str) -> str:
        """
        Calcula hash SHA-256 de um arquivo
        
        Args:
            file_path: Caminho para o arquivo
            
        Returns:
            Hash do arquivo em hexadecimal
        """
        hash_sha256 = hashlib.sha256()
        try:
            with open(file_path, 'rb') as file:
                for chunk in iter(lambda: file.read(4096), b""):
                    hash_sha256.update(chunk)
        except FileNotFoundError:
            raise FileNotFoundError(f"Arquivo não encontrado: {file_path}")
        
        return hash_sha256.hexdigest()
    
    @staticmethod
    def calculate_block_hash(block_data: bytes) -> str:
        """
        Calcula hash de um bloco específico
        
        Args:
            block_data: Dados do bloco
            
        Returns:
            Hash do bloco em hexadecimal
        """
        return hashlib.sha256(block_data).hexdigest()
    
    @staticmethod
    def get_file_size(file_path: str) -> int:
        """
        Obtém o tamanho de um arquivo em bytes
        
        Args:
            file_path: Caminho para o arquivo
            
        Returns:
            Tamanho do arquivo em bytes
        """
        try:
            return os.path.getsize(file_path)
        except FileNotFoundError:
            raise FileNotFoundError(f"Arquivo não encontrado: {file_path}")
    
    @staticmethod
    def calculate_total_blocks(file_path: str, block_size: int = BLOCK_SIZE) -> int:
        """
        Calcula o número total de blocos de um arquivo
        
        Args:
            file_path: Caminho para o arquivo  
            block_size: Tamanho de cada bloco
            
        Returns:
            Número total de blocos
        """
        file_size = FileUtils.get_file_size(file_path)
        return (file_size + block_size - 1) // block_size  # Ceiling division
    
    @staticmethod
    def save_peer_state(peer_id: str, state_data: Dict) -> bool:
        """
        Salva o estado de um peer em arquivo
        
        Args:
            peer_id: ID do peer
            state_data: Dados do estado para salvar
            
        Returns:
            True se sucesso, False caso contrário
        """
        try:
            state_file = os.path.join(LOGS_DIR, f"peer_{peer_id}_state.pkl")
            with open(state_file, 'wb') as file:
                pickle.dump(state_data, file)
            return True
        except Exception as e:
            print(f"Erro ao salvar estado do peer {peer_id}: {e}")
            return False
    
    @staticmethod
    def load_peer_state(peer_id: str) -> Optional[Dict]:
        """
        Carrega o estado de um peer de arquivo
        
        Args:
            peer_id: ID do peer
            
        Returns:
            Dados do estado ou None se não encontrado
        """
        try:
            state_file = os.path.join(LOGS_DIR, f"peer_{peer_id}_state.pkl")
            with open(state_file, 'rb') as file:
                return pickle.load(file)
        except FileNotFoundError:
            return None
        except Exception as e:
            print(f"Erro ao carregar estado do peer {peer_id}: {e}")
            return None
    
    @staticmethod
    def cleanup_peer_state(peer_id: str) -> bool:
        """
        Remove arquivo de estado de um peer
        
        Args:
            peer_id: ID do peer
            
        Returns:
            True se sucesso, False caso contrário
        """
        try:
            state_file = os.path.join(LOGS_DIR, f"peer_{peer_id}_state.pkl")
            if os.path.exists(state_file):
                os.remove(state_file)
            return True
        except Exception as e:
            print(f"Erro ao limpar estado do peer {peer_id}: {e}")
            return False

class FileInfo:
    """Classe para armazenar informações de arquivo"""
    
    def __init__(self, file_path: str, block_size: int = BLOCK_SIZE):
        self.file_path = file_path
        self.file_name = os.path.basename(file_path)
        self.file_size = FileUtils.get_file_size(file_path)
        self.file_hash = FileUtils.calculate_file_hash(file_path)
        self.block_size = block_size
        self.total_blocks = FileUtils.calculate_total_blocks(file_path, block_size)
        self.blocks = FileUtils.split_file_to_blocks(file_path, block_size)
        self.block_hashes = [FileUtils.calculate_block_hash(block) for block in self.blocks]
    
    def get_block_data(self, block_id: int) -> Optional[bytes]:
        """
        Obtém dados de um bloco específico
        
        Args:
            block_id: ID do bloco
            
        Returns:
            Dados do bloco ou None se inválido
        """
        if 0 <= block_id < len(self.blocks):
            return self.blocks[block_id]
        return None
    
    def validate_block(self, block_id: int, block_data: bytes) -> bool:
        """
        Valida se um bloco está correto
        
        Args:
            block_id: ID do bloco
            block_data: Dados do bloco
            
        Returns:
            True se válido, False caso contrário
        """
        if 0 <= block_id < len(self.block_hashes):
            return FileUtils.calculate_block_hash(block_data) == self.block_hashes[block_id]
        return False