import sys
import struct

def get_hash(key_bytes):
    # This is a simplified version of MurmurHash2 used by Kafka
    # Not a perfect replica, but illustrates the concept
    h = 0x9747B28C
    for byte in key_bytes:
        h = (h << 5) | (h >> 27)
        h ^= byte
    return h & 0xFFFFFFFF

keys = ['100', '101', '102', '103', '104', '105']
num_partitions = 3

for key in keys:
    key_bytes = key.encode('utf-8')
    hash_value = get_hash(key_bytes)
    partition = hash_value % num_partitions
    print(f"Chave '{key}' -> Hash: {hash_value} -> Partição: {partition}")