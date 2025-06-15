# peer/file_manager.py
import os
import hashlib

def split_file(file_path, block_size=1024):
    blocks = []
    with open(file_path, 'rb') as f:
        block_id = 0
        while True:
            block = f.read(block_size)
            if not block:
                break
            # Salva o bloco em disco (ex: block_0.bin)
            block_hash = hashlib.sha256(block).hexdigest()
            with open(f"block_{block_id}.bin", 'wb') as block_file:
                block_file.write(block)
            blocks.append(block_hash)
            block_id += 1
    return blocks, block_id  # Retorna hashes E o total de blocos

def reconstruct_file(blocks, total_blocks, output_path):
    # Verifica se todos os blocos estão presentes
    if len(blocks) != total_blocks:
        raise ValueError(f"Arquivo incompleto. Blocos obtidos: {len(blocks)}/{total_blocks}")
    
    # Ordena os blocos e monta o arquivo
    with open(output_path, 'wb') as f:
        for block_id in range(total_blocks):
            with open(f"block_{block_id}.bin", 'rb') as block_file:
                f.write(block_file.read())