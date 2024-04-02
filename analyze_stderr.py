from typing import Any
import numpy as np
import sys
import json
import re
import matplotlib.pyplot as plt

CHUNK = r"[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+"

def has_payload(line: str) -> bool:
    try:
        return "textPayload" in json.loads(line)
    except:
        return False

def get_payload(line: str) -> str:
    contents = json.loads(line)
    return contents["textPayload"]

def is_requesting_4d_chunk(payload: str, var: str) -> bool:
    return (
        payload.startswith(
            "GET: https://storage.googleapis.com/download/storage/v1/b/"
            "contrails-301217-sandbox-internal"
        ) and 
        ("pl.zarr" in payload or "sl.zarr" in payload) and
        re.search(CHUNK, payload) is not None and
        re.search(f"%2F{var}%2F", payload) is not None
    )

def parse_chunk(payload:str) -> bool:
    chunk = re.search(CHUNK, payload).group()
    return tuple(int(i) for i in chunk.split("."))

def process(lines: list[str], var:str) -> None:
    idxs = []
    for line in lines:
        if not has_payload(line):
            continue
        payload = get_payload(line)
        if not is_requesting_4d_chunk(payload, var):
            continue
        idx = parse_chunk(payload)
        idxs.append(idx)

    size = tuple(max(idx[i] for idx in idxs) + 1 for i in range(4))
    counts = np.zeros(size, dtype=int)
    for idx in idxs:
        counts[idx] += 1

    return counts
        

if __name__ == "__main__":

    fname = sys.argv[1]

    n_downloads = np.arange(1, 10)
    n_chunks = np.zeros(n_downloads.shape)
    for var in ["t", "q", "u", "v", "w", "ciwc"]:
        with open(fname, "r") as f:
            counts = process(f.readlines(), var)

        bin_edges = np.linspace(-0.5, counts.max() + 0.5, counts.max() + 2)
        n, _ = np.histogram(counts.ravel(), bin_edges)

        for downloads, chunks in enumerate(n):
            if downloads > 0:
                n_chunks[downloads - 1] += chunks

    avg = (n_downloads*n_chunks).sum() / n_chunks.sum()
    print(f"Model-level variables: {avg:.3f} downloads/chunk")
    
    n_downloads = np.arange(1, 10)
    n_chunks = np.zeros(n_downloads.shape)
    for var in ["tsr", "ttr"]:
        with open(fname, "r") as f:
            counts = process(f.readlines(), var)

        bin_edges = np.linspace(-0.5, counts.max() + 0.5, counts.max() + 2)
        n, _ = np.histogram(counts.ravel(), bin_edges)

        for downloads, chunks in enumerate(n):
            if downloads > 0:
                n_chunks[downloads - 1] += chunks

    avg = (n_downloads*n_chunks).sum() / n_chunks.sum()
    print(f"Single-level variables: {avg:.3f} downloads/chunk")
