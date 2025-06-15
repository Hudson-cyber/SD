#!/bin/bash
cd "$(dirname "$0")/.."

# Clean folders
rm -rf files/blocks/* files/downloads logs/*
mkdir -p files/blocks files/downloads logs

# Split the file using config settings
PYTHONPATH=. python3 -c "from utils.utils import partition_file; import config; config.TOTAL_BLOCKS = partition_file(f'files/{config.FILE_NAME}', 'files/blocks', config.BLOCK_SIZE)"

# Get total number of blocks
TOTAL_BLOCKS=$(ls files/blocks | wc -l)
PEERS=10
BLOCKS_PER_PEER=$((TOTAL_BLOCKS / PEERS))
EXTRA_BLOCKS=$((TOTAL_BLOCKS % PEERS))
ALL_BLOCKS=$(seq 0 $((TOTAL_BLOCKS - 1)))

# Distribute blocks to peers ensuring full coverage
i=0
for b in $ALL_BLOCKS; do
    echo $b >> temp_peer_$((i % PEERS)).txt
    i=$((i + 1))
done

# Start peers with their respective blocks
for i in $(seq 0 $((PEERS - 1))); do
    BLOCKS=$(shuf temp_peer_$i.txt | xargs)
    echo "Starting peer $i with blocks: $BLOCKS"
    python3 peer/peer.py $i $TOTAL_BLOCKS $BLOCKS &
    rm temp_peer_$i.txt
done

wait
