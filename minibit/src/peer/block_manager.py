import os
import hashlib
import json
import logging
from typing import Dict, List, Optional, Set
from pathlib import Path

logger = logging.getLogger(__name__)

class BlockManager:
    def __init__(self, peer_id: str, files_dir: str = "files"):
        self.peer_id = peer_id
        self.files_dir = Path(files_dir)
        self.original_dir = self.files_dir / "original"
        self.reconstructed_dir = self.files_dir / "reconstructed"
        self.blocks_dir = self.files_dir / f"peer_{peer_id}_blocks"
        
        # Criar diretórios se não existirem
        for dir_path in [self.original_dir, self.reconstructed_dir, self.blocks_dir]:
            dir_path.mkdir(parents=True, exist_ok=True)
        
        # Estado dos blocos
        self.blocks_owned: Set[int] = set()
        self.total_blocks: int = 0
        self.block_size: int = 1024  # 1KB por bloco
        self.file_name: Optional[str] = None
        self.file_hash: Optional[str] = None
        self.blocks_data: Dict[int, bytes] = {}
        
        # Carregar estado se existir
        self._load_state()
    
    def split_file_into_blocks(self, file_path: str, block_size: int = 1024) -> int:
        """Divide arquivo em blocos numerados"""
        try:
            file_path = Path(file_path)
            if not file_path.exists():
                raise FileNotFoundError(f"Arquivo não encontrado: {file_path}")
            
            self.file_name = file_path.name
            self.block_size = block_size
            
            # Calcular hash do arquivo original
            with open(file_path, 'rb') as f:
                content = f.read()
                self.file_hash = hashlib.sha256(content).hexdigest()
            
            # Dividir em blocos
            blocks = []
            for i in range(0, len(content), block_size):
                block_data = content[i:i + block_size]
                blocks.append(block_data)
            
            self.total_blocks = len(blocks)
            
            # Salvar blocos
            for block_id, block_data in enumerate(blocks):
                block_file = self.blocks_dir / f"block_{block_id}.dat"
                with open(block_file, 'wb') as f:
                    f.write(block_data)
                
                self.blocks_data[block_id] = block_data
                self.blocks_owned.add(block_id)
            
            self._save_state()
            logger.info(f"Arquivo {self.file_name} dividido em {self.total_blocks} blocos")
            return self.total_blocks
            
        except Exception as e:
            logger.error(f"Erro ao dividir arquivo: {e}")
            raise
    
    def has_block(self, block_id: int) -> bool:
        """Verifica se possui um bloco específico"""
        return block_id in self.blocks_owned
    
    def add_block(self, block_id: int, data: bytes) -> bool:
        """Adiciona um bloco recebido"""
        try:
            if block_id < 0 or block_id >= self.total_blocks:
                logger.warning(f"Block ID inválido: {block_id}")
                return False
            
            if block_id in self.blocks_owned:
                logger.debug(f"Bloco {block_id} já existe")
                return True
            
            # Salvar bloco
            block_file = self.blocks_dir / f"block_{block_id}.dat"
            with open(block_file, 'wb') as f:
                f.write(data)
            
            self.blocks_data[block_id] = data
            self.blocks_owned.add(block_id)
            
            self._save_state()
            logger.info(f"Bloco {block_id} adicionado com sucesso")
            
            # Verificar se arquivo está completo
            if len(self.blocks_owned) == self.total_blocks:
                logger.info("Todos os blocos recebidos! Reconstruindo arquivoSD.minibit.src..")
                self.reconstruct_file()
            
            return True
            
        except Exception as e:
            logger.error(f"Erro ao adicionar bloco {block_id}: {e}")
            return False
    
    def get_block(self, block_id: int) -> Optional[bytes]:
        """Obtém dados de um bloco"""
        if not self.has_block(block_id):
            return None
        
        if block_id in self.blocks_data:
            return self.blocks_data[block_id]
        
        # Carregar do disco se não estiver em memória
        try:
            block_file = self.blocks_dir / f"block_{block_id}.dat"
            if block_file.exists():
                with open(block_file, 'rb') as f:
                    data = f.read()
                    self.blocks_data[block_id] = data
                    return data
        except Exception as e:
            logger.error(f"Erro ao carregar bloco {block_id}: {e}")
        
        return None
    
    def get_missing_blocks(self) -> List[int]:
        """Retorna lista de blocos que ainda não possui"""
        if self.total_blocks == 0:
            return []
        
        all_blocks = set(range(self.total_blocks))
        missing = list(all_blocks - self.blocks_owned)
        missing.sort()
        return missing
    
    def get_block_rarity(self, known_peers_blocks: Dict[str, Set[int]]) -> Dict[int, int]:
        """Calcula raridade de cada bloco (quantos peers possuem)"""
        rarity = {}
        
        for block_id in range(self.total_blocks):
            count = 0
            for peer_blocks in known_peers_blocks.values():
                if block_id in peer_blocks:
                    count += 1
            rarity[block_id] = count
        
        return rarity
    
    def reconstruct_file(self, output_path: Optional[str] = None) -> bool:
        """Reconstitui o arquivo final"""
        try:
            if len(self.blocks_owned) != self.total_blocks:
                logger.warning(f"Arquivo incompleto: {len(self.blocks_owned)}/{self.total_blocks} blocos")
                return False
            
            if not self.file_name:
                logger.error("Nome do arquivo não definido")
                return False
            
            if not output_path:
                output_path = self.reconstructed_dir / self.file_name
            else:
                output_path = Path(output_path)
            
            # Juntar blocos em ordem
            with open(output_path, 'wb') as f:
                for block_id in range(self.total_blocks):
                    block_data = self.get_block(block_id)
                    if not block_data:
                        logger.error(f"Bloco {block_id} não encontrado")
                        return False
                    f.write(block_data)
            
            # Verificar integridade
            if self.file_hash:
                with open(output_path, 'rb') as f:
                    content = f.read()
                    reconstructed_hash = hashlib.sha256(content).hexdigest()
                    
                    if reconstructed_hash != self.file_hash:
                        logger.error("Hash do arquivo reconstruído não confere!")
                        return False
            
            logger.info(f"Arquivo reconstruído com sucesso: {output_path}")
            return True
            
        except Exception as e:
            logger.error(f"Erro ao reconstruir arquivo: {e}")
            return False
    
    def get_completion_percentage(self) -> float:
        """Retorna porcentagem de completude do arquivo"""
        if self.total_blocks == 0:
            return 0.0
        return (len(self.blocks_owned) / self.total_blocks) * 100
    
    def is_complete(self) -> bool:
        """Verifica se possui todos os blocos"""
        return len(self.blocks_owned) == self.total_blocks and self.total_blocks > 0
    
    def get_bitfield(self) -> bytes:
        """Retorna bitfield representando blocos possuídos"""
        if self.total_blocks == 0:
            return b''
        
        # Cada bit representa um bloco (1 = possui, 0 = não possui)
        bitfield_size = (self.total_blocks + 7) // 8  # Arredondar para cima
        bitfield = bytearray(bitfield_size)
        
        for block_id in self.blocks_owned:
            byte_idx = block_id // 8
            bit_idx = block_id % 8
            bitfield[byte_idx] |= (1 << (7 - bit_idx))
        
        return bytes(bitfield)
    
    def set_from_bitfield(self, bitfield: bytes, total_blocks: int):
        """Define blocos possuídos a partir de bitfield"""
        self.total_blocks = total_blocks
        self.blocks_owned.clear()
        
        for block_id in range(total_blocks):
            byte_idx = block_id // 8
            bit_idx = block_id % 8
            
            if byte_idx < len(bitfield):
                if bitfield[byte_idx] & (1 << (7 - bit_idx)):
                    self.blocks_owned.add(block_id)
    
    def _save_state(self):
        """Salva estado atual do gerenciador"""
        try:
            state_file = self.blocks_dir / "state.json"
            state = {
                'peer_id': self.peer_id,
                'blocks_owned': list(self.blocks_owned),
                'total_blocks': self.total_blocks,
                'block_size': self.block_size,
                'file_name': self.file_name,
                'file_hash': self.file_hash
            }
            
            with open(state_file, 'w') as f:
                json.dump(state, f, indent=2)
                
        except Exception as e:
            logger.error(f"Erro ao salvar estado: {e}")
    
    def _load_state(self):
        """Carrega estado salvo anteriormente"""
        try:
            state_file = self.blocks_dir / "state.json"
            if not state_file.exists():
                return
            
            with open(state_file, 'r') as f:
                state = json.load(f)
            
            self.blocks_owned = set(state.get('blocks_owned', []))
            self.total_blocks = state.get('total_blocks', 0)
            self.block_size = state.get('block_size', 1024)
            self.file_name = state.get('file_name')
            self.file_hash = state.get('file_hash')
            
            # Carregar blocos existentes
            for block_id in self.blocks_owned:
                block_file = self.blocks_dir / f"block_{block_id}.dat"
                if block_file.exists():
                    with open(block_file, 'rb') as f:
                        self.blocks_data[block_id] = f.read()
            
            logger.info(f"Estado carregado: {len(self.blocks_owned)}/{self.total_blocks} blocos")
            
        except Exception as e:
            logger.error(f"Erro ao carregar estado: {e}")
    
    def cleanup(self):
        """Limpa arquivos temporários"""
        try:
            import shutil
            if self.blocks_dir.exists():
                shutil.rmtree(self.blocks_dir)
            logger.info("Cleanup realizado com sucesso")
        except Exception as e:
            logger.error(f"Erro no cleanup: {e}")