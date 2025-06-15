for n in {1..5}; 
do
    python peer/peer_node.py --id peer$n --file data.txt --tracker http://localhost:$((n+4999))
done

