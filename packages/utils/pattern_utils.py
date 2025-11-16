import hashlib
from typing import List


def generate_pattern_hash(pattern_type: str, addresses: List[str]) -> str:
    sorted_addrs = sorted(addresses)
    pattern_string = f"{pattern_type}:{','.join(sorted_addrs)}"
    return hashlib.sha256(pattern_string.encode()).hexdigest()[:16]


def generate_pattern_id(pattern_type: str, pattern_hash: str) -> str:
    return f"{pattern_type}_{pattern_hash}"